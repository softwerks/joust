# Copyright 2020 Softwerks LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import functools
import http
import logging
import signal
import types
from typing import Optional, Type
import urllib.parse
import uuid

import aioredis
import websockets

from . import config

logger: logging.Logger = logging.getLogger(__name__)


class ServerProtocol(websockets.WebSocketServerProtocol):
    def __init__(self, redis: aioredis.Redis, *args, **kwargs):
        self.redis: aioredis.Redis = redis
        super().__init__(*args, **kwargs)

    async def process_request(
        self, path: str, request_headers: websockets.http.Headers
    ) -> Optional[websockets.server.HTTPResponse]:
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse(path)
        query_params: dict = urllib.parse.parse_qs(parsed_url.query)

        try:
            self.game_id: uuid.UUID = uuid.UUID(parsed_url.path.rsplit("/", 1)[-1])
        except ValueError:
            logger.info(f"Invalid or missing game ID: {parsed_url.path}")
            return (http.HTTPStatus.BAD_REQUEST, [], b"")

        try:
            self.token: str = query_params["token"][0]
        except KeyError:
            logger.info(f"Missing credentials: {query_params}")
            return (
                http.HTTPStatus.UNAUTHORIZED,
                [("WWW-Authenticate", "Token")],
                b"Missing credentials\n",
            )

        self.session_id: Optional[str] = await self.redis.get("websocket:" + self.token)
        if self.session_id is None:
            logger.info(f"Invalid token: {self.token}")
            return (
                http.HTTPStatus.UNAUTHORIZED,
                [("WWW-Authenticate", "Token")],
                b"Invalid credentials\n",
            )

        return await super().process_request(path, request_headers)


class Server:
    async def __aenter__(self) -> "Server":
        await self.startup()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[types.TracebackType],
    ) -> None:
        await self.shutdown()

    def _signal_workaround(self) -> None:
        """Workaround signal issues on Windows

        https://github.com/python/asyncio/issues/191
        https://github.com/python/asyncio/issues/407
        """
        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

        async def wakeup() -> None:
            while not self._shutdown.done():
                await asyncio.sleep(0.1)

        def signint_handler(signalnum: int, frame: types.FrameType):
            self._shutdown.set_result(None)

        signal.signal(signal.SIGINT, signint_handler)
        loop.create_task(wakeup())

    async def startup(self) -> None:
        self.redis: aioredis.Redis = await aioredis.create_redis_pool(config.REDIS)
        logger.info(f"Redis connected on {config.REDIS}")

        loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        self._shutdown: asyncio.Future = loop.create_future()
        try:
            for s in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(s, self._shutdown.set_result, None)
        except NotImplementedError:
            logger.warning("loop.add_signal_handler not supported, using workaround")
            self._signal_workaround()

    async def serve(self):
        if config.UNIX_SOCKET is not None:
            async with websockets.unix_serve(
                self._handler,
                config.UNIX_SOCKET,
                create_protocol=functools.partial(ServerProtocol, self.redis),
            ):
                logger.info(f"Running on {config.UNIX_SOCKET} (Press CTRL+C to quit)")
                await self._shutdown
        else:
            async with websockets.serve(
                self._handler,
                "localhost",
                config.PORT,
                create_protocol=functools.partial(ServerProtocol, self.redis),
                reuse_port=config.REUSE_PORT,
            ):
                logger.info(
                    f"Running on localhost:{config.PORT} (Press CTRL+C to quit)"
                )
                await self._shutdown
        logger.info("Server stopped")

    async def _handler(self, websocket: ServerProtocol, path: str):
        logger.info(f"{websocket.remote_address} - {websocket.game_id} [opened]")
        async for message in websocket:
            await websocket.send(message)
        logger.info(f"{websocket.remote_address} - {websocket.game_id} [closed]")

    async def shutdown(self) -> None:
        self.redis.close()
        await self.redis.wait_closed()
        logger.info("Server shutdown")

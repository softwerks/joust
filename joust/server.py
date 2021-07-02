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

import aioredis
import asyncio
import json
import logging
import signal
import socket
import types
from typing import Optional, Type
import os

import backgammon
import websockets

from joust import config
from joust import game
from joust import match
from joust import protocol
from joust import redis

logger: logging.Logger = logging.getLogger(__name__)


class Server:
    _shutdown: asyncio.Future

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

        async def wakeup() -> None:
            while not self._shutdown.done():
                await asyncio.sleep(0.1)

        def signint_handler(signalnum: int, frame: types.FrameType):
            self._shutdown.set_result(None)

        signal.signal(signal.SIGINT, signint_handler)
        asyncio.get_running_loop().create_task(wakeup())

    async def startup(self) -> None:
        self._shutdown = asyncio.get_running_loop().create_future()
        try:
            for s in [signal.SIGINT, signal.SIGTERM]:
                asyncio.get_running_loop().add_signal_handler(
                    s, self._shutdown.set_result, None
                )
        except NotImplementedError:
            logger.warning("loop.add_signal_handler not supported, using workaround")
            self._signal_workaround()

    async def serve(self):
        LISTEN_FDS: int = int(os.getenv("LISTEN_FDS", 0))
        if LISTEN_FDS > 0:  # file descriptor(s) passed by the service manager
            if LISTEN_FDS > 1:
                raise ValueError(
                    f"More than one ({LISTEN_FDS}) file descriptor was passed by the service manager"
                )
            SD_LISTEN_FDS_START: int = 3
            sock: socket.socket = socket.socket(fileno=SD_LISTEN_FDS_START)
            async with websockets.unix_serve(
                self._handler,
                path=None,
                sock=sock,
                create_protocol=protocol.ServerProtocol,
                subprotocols=["game", "match"],
            ):
                logger.info(f"Running on {sock.getsockname()}")
                await self._shutdown
        elif config.UNIX_SOCKET is not None:
            async with websockets.unix_serve(
                self._handler,
                config.UNIX_SOCKET,
                create_protocol=protocol.ServerProtocol,
                subprotocols=["game", "match"],
            ):
                logger.info(f"Running on {config.UNIX_SOCKET} (Press CTRL+C to quit)")
                await self._shutdown
        else:
            async with websockets.serve(
                self._handler,
                "localhost",
                config.PORT,
                create_protocol=protocol.ServerProtocol,
                reuse_port=config.REUSE_PORT,
                subprotocols=["game", "match"],
            ):
                logger.info(
                    f"Running on localhost:{config.PORT} (Press CTRL+C to quit)"
                )
                await self._shutdown
        logger.info("Server stopped")

    async def _handler(self, websocket: protocol.ServerProtocol, path: str) -> None:
        remote_address: str = self._remote_address(websocket)

        logger.info(f"{remote_address} [opened]")

        try:
            await self._increment_connected()

            if websocket.subprotocol == "game":
                await game.handler(websocket, path)
            elif websocket.subprotocol == "match":
                await match.handler(websocket, path)
        except websockets.exceptions.ConnectionClosedError as e:
            logger.info(f"{remote_address} [closed abnormally]")
        finally:
            await self._decrement_connected()
            logger.info(f"{remote_address} [closed]")

    def _remote_address(self, websocket: protocol.ServerProtocol) -> str:
        if "X-Real-IP" in websocket.request_headers:
            return websocket.request_headers["X-Real-IP"]
        elif websocket.remote_address is not None:
            return websocket.remote_address[0]
        else:
            return "unknown"

    async def _increment_connected(self) -> int:
        conn: aioredis.Redis = await redis.get_connection()
        return await conn.incr("stats:connected")

    async def _decrement_connected(self) -> int:
        conn: aioredis.Redis = await redis.get_connection()
        return await conn.decr("stats:connected")

    async def shutdown(self) -> None:
        await redis.close()
        logger.info("Server shutdown")

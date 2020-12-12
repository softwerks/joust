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
import base64
import http
import logging
import signal
from typing import Optional
import urllib.parse
import uuid

import websockets

from . import config

logger: logging.Logger = logging.getLogger(__name__)


class ServerProtocol(websockets.WebSocketServerProtocol):
    async def process_request(
        self, path: str, request_headers: websockets.http.Headers
    ) -> Optional[websockets.server.HTTPResponse]:
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse(path)
        query_params: dict = urllib.parse.parse_qs(parsed_url.query)

        try:
            game_id: uuid.UUID = uuid.UUID(parsed_url.path.rsplit("/", 1)[-1])
        except ValueError:
            logger.info(f"Invalid or missing game ID: {parsed_url.path}")
            return (http.HTTPStatus.BAD_REQUEST, [], b"")

        try:
            token: str = query_params["token"][0]
        except KeyError:
            logger.info(f"Missing credentials: {query_params}")
            return (
                http.HTTPStatus.UNAUTHORIZED,
                [("WWW-Authenticate", "Token")],
                b"Missing credentials\n",
            )

        # authenticate user

        self.game_id = game_id
        self.token = token

        return await super().process_request(path, request_headers)


class Server:
    def __init__(self):
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.shutdown: asyncio.Future = self.loop.create_future()
        for s in [signal.SIGINT, signal.SIGTERM]:
            self.loop.add_signal_handler(s, self.shutdown.set_result, None)

    async def handler(self, websocket: ServerProtocol, path: str):
        logger.info(f"{websocket.remote_address} - {websocket.game_id} [opened]")
        async for message in websocket:
            await websocket.send(message)
        logger.info(f"{websocket.remote_address} - {websocket.game_id} [closed]")

    async def serve(self):
        if config.UNIX_SOCKET is not None:
            logger.info(f"Running on {config.UNIX_SOCKET}")
            async with websockets.unix_serve(
                self.handler, config.UNIX_SOCKET, create_protocol=ServerProtocol
            ):
                await self.shutdown
        else:
            logger.info(f"Running on localhost:{config.PORT}")
            async with websockets.serve(
                self.handler,
                "localhost",
                config.PORT,
                create_protocol=ServerProtocol,
                reuse_port=True,
            ):
                await self.shutdown

    def run(self):
        logger.info(f"Starting server (Press CTRL+C to quit)")
        self.loop.run_until_complete(self.serve())
        logger.info("Shutting down")
        self.loop.stop()
        self.loop.close()

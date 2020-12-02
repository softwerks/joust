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
import logging
import signal
import websockets

import joust

logger: logging.Logger = logging.getLogger(__name__)


class Server:
    def __init__(self, *, port: int = 5555):
        self.port: int = port
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.shutdown: asyncio.Future = self.loop.create_future()
        self.loop.add_signal_handler(signal.SIGINT, self.shutdown.set_result, None)

    async def handler(
        self, websocket: websockets.server.WebSocketServerProtocol, path: str
    ):
        logger.info(f"{websocket.remote_address} - {path} [opened]")
        async for message in websocket:
            await websocket.send(message)
        logger.info(f"{websocket.remote_address} - {path} [closed]")

    async def serve(self):
        if joust.config.UNIX_SOCKET is not None:
            logger.info(f"Running on {joust.config.UNIX_SOCKET}")
            async with websockets.unix_serve(self.handler, joust.config.UNIX_SOCKET):
                await self.shutdown
        else:
            logger.info(f"Running on localhost:{self.port}")
            async with websockets.serve(self.handler, "localhost", self.port):
                await self.shutdown

    def run(self):
        logger.info(f"Starting server (Press CTRL+C to quit)")
        self.loop.run_until_complete(self.serve())
        logger.info("Shutting down")
        self.loop.stop()
        self.loop.close()

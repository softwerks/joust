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
import contextlib
import logging
from typing import AsyncGenerator, Dict, Set

import aioredis

from . import redis
from . import protocol

logger: logging.Logger = logging.getLogger(__name__)

channels: Dict[str, Set[protocol.ServerProtocol]] = {}


async def reader(message_queue: aioredis.Channel) -> None:
    global channels

    c: str = message_queue.name.decode()

    logger.info(f"Started listening to {c}")
    async for message in message_queue.iter(encoding="utf-8"):
        logger.info(f"Broadcasting to {c}: {str(message)}")
        for websocket in channels[c]:
            await websocket.send(str(message))
    logger.info(f"Stopped listening to {c}")


async def subscribe(websocket: protocol.ServerProtocol) -> None:
    global channels

    c: str = str(websocket.game_id)

    if c not in channels:
        channels[c] = set()
        async with redis.get_connection() as conn:
            message_queue: aioredis.Channel = (await conn.subscribe(c))[0]
            logger.info(f"Subscribed to {c}")
        asyncio.get_running_loop().create_task(reader(message_queue))

    channels[c].add(websocket)
    logger.info(f"{websocket.remote_address} - {c} [subscribed]")


async def unsubscribe(websocket: protocol.ServerProtocol) -> None:
    global channels

    c: str = str(websocket.game_id)

    channels[c].remove(websocket)
    logger.info(f"{websocket.remote_address} - {c} [unsubscribed]")

    if not channels[c]:
        async with redis.get_connection() as conn:
            await conn.unsubscribe(c)
            logger.info(f"Unsubscribed from {c}")
        del channels[c]


@contextlib.asynccontextmanager
async def get_channel(websocket: protocol.ServerProtocol) -> AsyncGenerator[None, None]:
    await subscribe(websocket)
    try:
        yield
    finally:
        await unsubscribe(websocket)

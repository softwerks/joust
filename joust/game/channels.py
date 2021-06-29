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

from joust import redis
from joust.protocol import ServerProtocol

logger: logging.Logger = logging.getLogger(__name__)

channels: Dict[str, Set[ServerProtocol]] = {}


async def reader(message_queue: aioredis.Channel, game_id: str) -> None:
    global channels

    logger.info(f"Started listening to {game_id}")
    async for message in message_queue.iter(encoding="utf-8"):
        logger.info(f"Broadcasting to {game_id}: {str(message)}")
        for websocket in channels[game_id]:
            await websocket.send(str(message))
    logger.info(f"Stopped listening to {game_id}")


async def subscribe(websocket: ServerProtocol, game_id: str) -> None:
    global channels

    if game_id not in channels:
        channels[game_id] = set()
        conn: aioredis.Redis = await redis.get_connection()
        message_queue: aioredis.Channel = (await conn.subscribe(game_id))[0]
        logger.info(f"Subscribed to {game_id}")
        asyncio.get_running_loop().create_task(reader(message_queue, game_id))

    channels[game_id].add(websocket)
    logger.info(f"{websocket.remote_address} - {game_id} [subscribed]")


async def unsubscribe(websocket: ServerProtocol, game_id: str) -> None:
    global channels

    channels[game_id].remove(websocket)
    logger.info(f"{websocket.remote_address} - {game_id} [unsubscribed]")

    if not channels[game_id]:
        conn: aioredis.Redis = await redis.get_connection()
        await conn.unsubscribe(game_id)
        logger.info(f"Unsubscribed from {game_id}")
        del channels[game_id]


@contextlib.asynccontextmanager
async def get_channel(
    websocket: ServerProtocol, game_id: str
) -> AsyncGenerator[None, None]:
    await subscribe(websocket, game_id)
    try:
        yield
    finally:
        await unsubscribe(websocket, game_id)

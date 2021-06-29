# Copyright 2021 Softwerks LLC
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
import enum
import json
import logging

import aioredis

from joust.protocol import ServerProtocol
from joust import redis
from joust import session

logger: logging.Logger = logging.getLogger(__name__)


@enum.unique
class Opcode(enum.Enum):
    ADD: str = "add"
    REM: str = "rem"


async def handler(websocket: ServerProtocol, path: str) -> None:
    done, pending = await asyncio.wait(
        [_queue(websocket), _handle_message(websocket)],
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()

    await asyncio.gather(*pending)


async def _queue(websocket: ServerProtocol) -> None:
    user: session.Session = await session.load(websocket.session_token)
    conn: aioredis.Redis = await redis.get_connection()

    await conn.publish(
        "tournament:queue", json.dumps({"opcode": Opcode.ADD.value, "user": user.id_})
    )

    channel: str = f"tournament:{user.id_}"
    message_queue: aioredis.Channel = (await conn.subscribe(channel))[0]

    try:
        game_id: str = await message_queue.get(encoding="utf-8")
        logger.info(game_id)
    except asyncio.CancelledError:
        await conn.publish(
            "tournament:queue",
            json.dumps({"opcode": Opcode.REM.value, "user": user.id_}),
        )
    finally:
        await conn.unsubscribe(channel)


async def _handle_message(websocket: ServerProtocol) -> None:
    try:
        async for message in websocket:
            pass
    except asyncio.CancelledError:
        pass

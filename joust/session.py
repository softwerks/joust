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
import contextlib
import dataclasses
import logging
from typing import AsyncGenerator, Dict, Optional
import uuid

from . import redis

logger: logging.Logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Session:
    address: str
    authenticated: bool = dataclasses.field(init=False)
    created: str
    id_: str = dataclasses.field(init=False)
    last_seen: str
    session_id: str
    session_type: str
    user_agent: str
    game_id: Optional[str] = None
    time_zone: Optional[str] = None
    user_id: Optional[str] = None

    def __post_init__(self) -> None:
        if self.user_id is not None:
            self.authenticated = True
            self.id_ = self.user_id
        else:
            self.authenticated = False
            self.id_ = self.session_id

    async def _lookup_game_id(self) -> None:
        async with redis.get_connection() as conn:
            self.game_id = await conn.hget("games", self.id_)

    async def join_game(self, game_id: uuid.UUID, player: int) -> bool:
        success: bool = False

        async with redis.get_connection() as conn:
            success = bool(
                await conn.hsetnx(f"game:{game_id}", f"player_{player}", self.id_)
            )
            if success:
                if self.authenticated:
                    await conn.hset("games", self.id_, str(game_id))
                else:
                    await conn.hset(
                        f"session:{self.session_id}", "game_id", str(game_id)
                    )

        return success

    async def leave_game(self, game_id: uuid.UUID) -> None:
        if uuid.UUID(self.game_id) == game_id:
            async with redis.get_connection() as conn:
                if self.authenticated:
                    await conn.hdel("games", self.id_)
                else:
                    await conn.hdel(f"session:{self.session_id}", "game_id")
            self.game_id = None


@contextlib.asynccontextmanager
async def load(session_id: str) -> AsyncGenerator[Session, None]:
    async with redis.get_connection() as conn:
        session_data: Dict[str, str] = await conn.hgetall(
            f"session:{session_id}", encoding="utf-8"
        )

    session: Session = Session(session_id=session_id, **session_data)

    if session.authenticated:
        await session._lookup_game_id()

    yield session

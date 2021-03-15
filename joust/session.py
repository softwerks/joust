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
    last_seen: str
    session_id: str
    session_type: str
    user_agent: str
    game_id: Optional[str] = None
    time_zone: Optional[str] = None
    user_id: Optional[str] = None

    def __post_init__(self) -> None:
        self.authenticated = True if self.session_type == "authenticated" else False

    async def _lookup_game_id(self) -> None:
        if self.user_id is not None:
            async with redis.get_connection() as conn:
                self.game_id = await conn.hget("games", self.user_id)
        else:
            raise LookupError(
                f"Authenticated session missing user_id: {self.session_id}"
            )

    async def join_game(self, game_id: uuid.UUID, player: int) -> bool:
        result: bool = False

        async with redis.get_connection() as conn:
            if self.authenticated:
                if self.user_id is not None:
                    result = conn.hsetnx(
                        f"game:{game_id}", f"player_{player}", self.user_id
                    )
                    if result:
                        conn.hset("games", self.user_id, str(game_id))
                else:
                    logger.error(
                        f"Authenticated session missing user_id: {self.session_id}"
                    )
            else:
                result = conn.hsetnx(
                    f"game:{game_id}", f"player_{player}", self.session_id
                )
                if result:
                    conn.hset(f"session:{self.session_id}", "game_id", str(game_id))

        return result


@contextlib.asynccontextmanager
async def load(session_id: str) -> AsyncGenerator[Session, None]:
    session_data: Dict[str, str]

    async with redis.get_connection() as conn:
        session_data = await conn.hgetall(f"session:{session_id}", encoding="utf-8")

    session: Session = Session(session_id=session_id, **session_data)

    if session.authenticated:
        try:
            await session._lookup_game_id()
        except LookupError as error:
            logger.error(error)

    yield session

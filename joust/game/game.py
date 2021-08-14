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

import dataclasses
import enum
import time
from typing import Dict, Optional

import aioredis
import backgammon

from joust import redis


MATCH_TIME: int = 120000  # 2 minutes / point
DELAY_TIME: int = 15000  # 15 seconds / turn


@enum.unique
class Status(enum.Enum):
    CONNECTED: str = "connected"
    DISCONNECTED: str = "disconnected"
    FORFEIT: str = "forfeit"


@dataclasses.dataclass
class Game:
    id_: str
    position: str
    match: str
    state: backgammon.Backgammon = dataclasses.field(init=False)
    player_0: Optional[str] = None
    player_1: Optional[str] = None
    status_0: Optional[str] = None
    status_1: Optional[str] = None
    time_0: str = "0"
    time_1: str = "0"
    timestamp: str = "0"

    def __post_init__(self) -> None:
        self.state = backgammon.Backgammon(self.position, self.match)

    async def update(self) -> None:
        """Update the game in redis."""
        conn: aioredis.Redis = await redis.get_connection()
        key: str = f"game:{self.id_}"
        pipeline: aioredis.commands.transaction.MultiExec = conn.multi_exec()
        pipeline.hset(key, "position", self.state.position.encode())
        pipeline.hset(key, "match", self.state.match.encode())
        if self.time_0 is not None:
            pipeline.hset(key, "time_0", self.time_0)
        if self.time_1 is not None:
            pipeline.hset(key, "time_1", self.time_1)
        if self.timestamp is not None:
            pipeline.hset(key, "timestamp", self.timestamp)
        await pipeline.execute()

    async def get_player(self, session_id: str) -> Optional[int]:
        """Return the user's player ID or None."""
        player: Optional[int] = None

        if session_id == self.player_0:
            player = 0
        elif session_id == self.player_1:
            player = 1

        return player

    async def join_game(self, session_id: str) -> Optional[int]:
        """Try to add the player to the game and return the player ID."""
        player: Optional[int] = None

        conn: aioredis.Redis = await redis.get_connection()
        if self.player_0 is None:
            if await conn.hsetnx(f"game:{self.id_}", f"player_0", session_id):
                player = 0
                self.player_0 = session_id
        elif self.player_1 is None:
            if await conn.hsetnx(f"game:{self.id_}", f"player_1", session_id):
                player = 1
                self.player_1 = session_id

        return player

    def get_turn(self) -> Optional[str]:
        """Return the user ID of the turn owner or None."""
        return self.player_0 if self.state.match.turn.value == 0 else self.player_1

    async def set_status(self, session_id: str, status: Status) -> bool:
        """Update the player's status and return success."""
        status_updated: bool = False

        player: Optional[int] = await self.get_player(session_id)
        if player is not None:
            conn: aioredis.Redis = await redis.get_connection()
            if player == 0:
                await conn.hset(f"game:{self.id_}", "status_0", status.value)
                self.status_0 = status.value
            elif player == 1:
                await conn.hset(f"game:{self.id_}", "status_1", status.value)
                self.status_1 = status.value
            status_updated = True

        return status_updated

    async def delete(self) -> None:
        """Delete the game."""
        conn: aioredis.Redis = await redis.get_connection()
        await conn.delete(f"game:{self.id_}")

    def ms_timestamp(self) -> int:
        """Return the time in milliseconds since the epoch."""
        return int(time.time() * 1000)

    def start_clock(self) -> None:
        """Set the players' clocks and record the current time."""
        self.time_0 = self.time_1 = str(self.state.match.length * MATCH_TIME)
        self.timestamp = str(self.ms_timestamp())

    def swap_clock(self, turn_owner: int) -> None:
        """Stop the turn owner's clock and start the opponent's clock."""
        new_timestamp: int = self.ms_timestamp()

        elapsed: int = max(0, (new_timestamp - int(self.timestamp)) - DELAY_TIME)

        if turn_owner == 0:
            self.time_0 = str(int(self.time_0) - elapsed)
        else:
            self.time_1 = str(int(self.time_1) - elapsed)

        self.timestamp = str(new_timestamp)


async def load(game_id: str) -> Game:
    """Load and return the game."""
    conn: aioredis.Redis = await redis.get_connection()
    game_data: Dict[str, str] = await conn.hgetall(f"game:{game_id}", encoding="utf-8")

    if not game_data:
        raise ValueError(f"Game not found: {game_id}")

    return Game(id_=game_id, **game_data)

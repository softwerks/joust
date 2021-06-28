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
from typing import Dict, Optional

import backgammon

from joust import redis


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

    def __post_init__(self) -> None:
        self.state = backgammon.Backgammon(self.position, self.match)

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

        async with redis.get_connection() as conn:
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
            async with redis.get_connection() as conn:
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
        async with redis.get_connection() as conn:
            await conn.delete(f"game:{self.id_}")


async def load(game_id: str) -> Game:
    """Load and return the game."""
    async with redis.get_connection() as conn:
        game_data: Dict[str, str] = await conn.hgetall(
            f"game:{game_id}", encoding="utf-8"
        )

    if not game_data:
        raise ValueError(f"Game not found: {game_id}")

    return Game(id_=game_id, **game_data)

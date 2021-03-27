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

import enum
import json
import jsonschema
import logging
from typing import Any, Dict, Optional, Tuple, Union
import uuid

import aioredis
import backgammon

from . import redis
from . import session

logger: logging.Logger = logging.getLogger(__name__)


@enum.unique
class Opcode(enum.Enum):
    JOIN: str = "join"
    MOVE: str = "move"
    SKIP: str = "skip"
    READY: str = "ready"
    ROLL: str = "roll"


@enum.unique
class Status(enum.Enum):
    READY: str = "ready"


payload_schema: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "opcode": {"type": "string", "enum": [e.value for e in Opcode],},
        "move": {
            "type": "array",
            "minItems": 2,
            "maxItems": 8,
            "items": {"type": ["integer", "null"]},
        },
        "player": {"type": "integer", "minimum": 0, "maximum": 1},
    },
    "required": ["opcode"],
}


def deserialize(serialized_payload: Union[str, bytes]) -> Dict[str, Any]:
    try:
        return json.loads(serialized_payload)
    except json.JSONDecodeError as error:
        logger.warning(error)
        raise ValueError("Payload is not a valid JSON document")


def validate(deserialized_payload: Dict[str, Any]) -> None:
    try:
        jsonschema.validate(instance=deserialized_payload, schema=payload_schema)
    except jsonschema.exceptions.ValidationError as error:
        logger.warning(error)
        raise ValueError("Invalid payload")


async def load_game(game_id: uuid.UUID) -> Dict[str, str]:
    async with redis.get_connection() as conn:
        game: Dict[str, str] = await conn.hgetall(f"game:{game_id}", encoding="utf-8")
    return game


async def join(
    game_id: uuid.UUID, session_id: str, game: Dict[str, str], bg: backgammon.Backgammon
) -> Optional[int]:
    async with session.load(session_id) as s:
        if bg.match.game_state is backgammon.match.GameState.NOT_STARTED:
            if s.game_id is None:
                player: int = 0 if "player_0" not in game else 1
                if await s.join_game(game_id, player):
                    return player
        elif uuid.UUID(s.game_id) == game_id:
            player_id: str = s.user_id if s.user_id else session_id
            if game.get("player_0") == player_id:
                return 0
            elif game.get("player_1") == player_id:
                return 1

    return None


async def start(
    game_id: uuid.UUID,
    session_id: str,
    player: int,
    game: Dict[str, str],
    bg: backgammon.Backgammon,
) -> bool:
    started: bool = False

    if bg.match.game_state is backgammon.match.GameState.NOT_STARTED:
        async with session.load(session_id) as s:
            authorized: bool = False

            authorized_player: Optional[str] = game.get(f"player_{player}")
            if s.authenticated and authorized_player == s.user_id:
                authorized = True
            elif authorized_player == s.session_id:
                authorized = True

            if authorized:
                async with redis.get_connection() as conn:
                    pipeline: aioredis.commands.transaction.MultiExec = conn.multi_exec()
                    pipeline.hset(
                        f"game:{game_id}", f"status_{player}", f"{Status.READY.value}"
                    )
                    opponent: int = 0 if player == 1 else 1
                    pipeline.hget(f"game:{game_id}", f"status_{opponent}", encoding="utf-8")
                    _, opponent_status = await pipeline.execute()
                    if opponent_status and Status(opponent_status) is Status.READY:
                        bg.match.game_state = backgammon.match.GameState.PLAYING
                        bg.first_roll()
                        started = True

    return started


def play(
    opcode: Opcode, deserialized_payload: Dict[str, Any], bg: backgammon.Backgammon
) -> None:
    def skip() -> None:
        try:
            bg.skip()
            bg.roll()
        except backgammon.backgammon.BackgammonError:
            raise ValueError("Cannot skip turn")

    def move() -> None:
        try:
            bg.play(
                tuple(
                    tuple(deserialized_payload["move"][i : i + 2])
                    for i in range(0, len(deserialized_payload["move"]), 2)
                )
            )
            bg.end_turn()
            bg.roll()
        except backgammon.backgammon.BackgammonError:
            raise ValueError(f"Invalid move: {deserialized_payload['move']}")

    if opcode is Opcode.SKIP:
        skip()
    elif opcode is Opcode.MOVE:
        move()


async def update_game(game_id: uuid.UUID, bg: backgammon.Backgammon) -> None:
    async with redis.get_connection() as conn:
        pipeline: aioredis.commands.transaction.MultiExec = conn.multi_exec()
        pipeline.hset(f"game:{game_id}", "position", bg.position.encode())
        pipeline.hset(f"game:{game_id}", "match", bg.match.encode())
        await pipeline.execute()


async def evaluate(
    game_id: uuid.UUID, session_id: str, deserialized_payload: Dict[str, Any]
) -> Tuple[bool, str]:
    bg: backgammon.Backgammon
    game: Dict[str, str]
    msg: Dict[str, Any] = {}
    opcode: Opcode
    publish: bool = False

    game = await load_game(game_id)
    bg = backgammon.Backgammon(game["position"], game["match"])

    opcode = Opcode(deserialized_payload["opcode"])
    if opcode is Opcode.JOIN:
        msg["player"] = await join(game_id, session_id, game, bg)
    elif opcode is Opcode.READY:
        player: Optional[int] = deserialized_payload.get("player")
        if player is not None:
            started: bool = await start(game_id, session_id, player, game, bg)
            if started:
                await update_game(game_id, bg)
                publish = True
        else:
            raise ValueError("Missing player ID")
    elif bg.match.game_state is backgammon.match.GameState.PLAYING:
        turn: str = game[f"player_{bg.match.player.value}"]
        if turn == session_id:
            play(opcode, deserialized_payload, bg)
            await update_game(game_id, bg)
            publish = True
        else:
            raise ValueError(f"Invalid player: {session_id} expecting {turn}")
    else:
        raise ValueError(f"Game isn't active: {game_id}")

    msg["game"] = bg.to_json()

    return publish, json.dumps(msg)


async def process_payload(
    game_id: uuid.UUID, session_id: str, serialized_payload: Union[str, bytes]
) -> Tuple[bool, str]:
    deserialized_payload: Dict[str, Any] = deserialize(serialized_payload)
    validate(deserialized_payload)
    return await evaluate(game_id, session_id, deserialized_payload)

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
from typing import Any, Dict, Tuple, Union
import uuid

import aioredis
import backgammon

from . import redis

logger: logging.Logger = logging.getLogger(__name__)


@enum.unique
class Opcode(enum.Enum):
    JOIN: str = "join"
    MOVE: str = "move"
    SKIP: str = "skip"
    ROLL: str = "roll"


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


async def join(game_id: uuid.UUID, session_id: str) -> None:
    pass


async def load_game(game_id: uuid.UUID) -> Dict[str, str]:
    async with redis.get_connection() as conn:
        game: Dict[str, str] = await conn.hgetall(f"game:{game_id}", encoding="utf-8")
    return game


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
    publish: bool

    game: Dict[str, str] = await load_game(game_id)
    bg: backgammon.Backgammon = backgammon.Backgammon(game["position"], game["match"])

    opcode: Opcode = Opcode(deserialized_payload["opcode"])
    if opcode is Opcode.JOIN:
        await join(game_id, session_id)
        publish = False
    elif bg.match.game_state is backgammon.match.GameState.PLAYING:
        turn: int = game[f"player_{game.match.player.value}"]
        if turn == session_id:
            play(opcode, deserialized_payload, bg)
            await update_game(game_id, bg)
            publish = True
        else:
            raise ValueError(f"Invalid player: {session_id} expecting {turn}")
    else:
        raise ValueError(f"Game isn't active: {game_id}")

    return publish, bg.to_json()


async def process_payload(
    game_id: uuid.UUID, session_id: str, serialized_payload: Union[str, bytes]
) -> Tuple[bool, str]:
    deserialized_payload: Dict[str, Any] = deserialize(serialized_payload)
    validate(deserialized_payload)
    return await evaluate(game_id, session_id, deserialized_payload)

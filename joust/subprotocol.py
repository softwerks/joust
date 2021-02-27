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
from typing import Any, Dict, Union
import uuid

import aioredis
import backgammon

from . import redis

logger: logging.Logger = logging.getLogger(__name__)


@enum.unique
class Opcode(enum.Enum):
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


async def process_payload(
    game_id: uuid.UUID, session_id: str, serialized_payload: Union[str, bytes]
) -> str:
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

    async def evaluate(deserialized_payload: Dict[str, Any]) -> str:
        async with redis.get_connection() as conn:
            state: Dict[str, str] = await conn.hgetall(
                f"game:{game_id}", encoding="utf-8"
            )
        game: backgammon.Backgammon = backgammon.Backgammon(
            state["position"], state["match"]
        )

        if state[f"player_{game.match.player.value}"] != session_id:
            raise ValueError(
                f"Invalid player: {session_id} expecting {state[f'player_{game.match.player.value}']}"
            )

        opcode: Opcode = Opcode(deserialized_payload["opcode"])
        if opcode is Opcode.SKIP:
            try:
                game.skip()
                game.roll()
            except backgammon.backgammon.BackgammonError:
                raise ValueError("Cannot skip turn")
        elif opcode is Opcode.MOVE:
            try:
                game.play(
                    tuple(
                        tuple(deserialized_payload["move"][i : i + 2])
                        for i in range(0, len(deserialized_payload["move"]), 2)
                    )
                )
                game.end_turn()
                game.roll()
            except backgammon.backgammon.BackgammonError:
                raise ValueError(f"Invalid move: {deserialized_payload['move']}")

        async with redis.get_connection() as conn:
            pipeline: aioredis.commands.transaction.MultiExec = conn.multi_exec()
            pipeline.hset(f"game:{game_id}", "position", game.position.encode())
            pipeline.hset(f"game:{game_id}", "match", game.match.encode())
            await pipeline.execute()
        return game.to_json()

    deserialized_payload: Dict[str, Any] = deserialize(serialized_payload)
    validate(deserialized_payload)
    return await evaluate(deserialized_payload)

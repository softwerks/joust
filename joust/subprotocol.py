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
import functools
import json
import jsonschema
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import uuid

import aioredis
import backgammon

from . import redis
from . import session

logger: logging.Logger = logging.getLogger(__name__)

PayloadType = Dict[str, Any]
ResponseType = Tuple[bool, PayloadType]


@enum.unique
class Opcode(enum.Enum):
    JOIN: str = "join"
    MOVE: str = "move"
    ROLL: str = "roll"
    SKIP: str = "skip"


@enum.unique
class ResponseCode(enum.Enum):
    ERROR: str = "error"
    PLAYER: str = "player"
    UPDATE: str = "update"


payload_schema: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "opcode": {
            "type": "string",
            "enum": [e.value for e in Opcode],
        },
        "move": {
            "type": "array",
            "minItems": 2,
            "maxItems": 8,
            "items": {"type": ["integer", "null"]},
        },
    },
    "required": ["opcode"],
}


def _authorized(func: Callable) -> Callable:
    @functools.wraps(func)
    async def wrapper(
        game_id: uuid.UUID, session_id: str, *args, **kwargs
    ) -> Union[Callable, ResponseType]:
        publish: bool = False

        async with session.load(session_id) as s:
            if s.game_id == str(game_id):
                game: Dict[str, str] = await _load_game(game_id)
                bg: backgammon.Backgammon = _decode_game(game)

                if s.id_ == game.get(f"player_{bg.match.turn.value}"):
                    return await func(game_id, session_id, s, game, bg, *args, **kwargs)

        return publish, {"code": ResponseCode.ERROR.value, "error": "Unauthorized"}

    return wrapper


async def process_payload(
    game_id: uuid.UUID, session_id: str, serialized_payload: Union[str, bytes]
) -> List[Tuple[bool, str]]:
    """Process the payload and return a list of responses."""
    payload: PayloadType = _deserialize(serialized_payload)
    _validate(payload)
    return await _evaluate(game_id, session_id, payload)


def _deserialize(serialized_payload: Union[str, bytes]) -> PayloadType:
    """Deserialze and return the payload."""
    try:
        return json.loads(serialized_payload)
    except json.JSONDecodeError as error:
        raise ValueError(error)


def _validate(payload: PayloadType) -> None:
    """Validate the payload."""
    try:
        jsonschema.validate(instance=payload, schema=payload_schema)
    except jsonschema.exceptions.ValidationError as error:
        raise ValueError(error)


async def _evaluate(
    game_id: uuid.UUID, session_id: str, payload: PayloadType
) -> List[Tuple[bool, str]]:
    """Evalualuate the payload and return a list of JSON responses."""
    responses: List[ResponseType] = []

    opcode: Opcode = Opcode(payload["opcode"])

    if opcode is Opcode.JOIN:
        responses.extend(await _join(game_id, session_id))
    elif opcode is Opcode.MOVE:
        try:
            responses.append(await _move(game_id, session_id, payload["move"]))
        except KeyError:
            raise ValueError("Missing move")
    elif opcode is Opcode.ROLL:
        responses.append(await _roll(game_id, session_id))
    elif opcode is Opcode.SKIP:
        responses.append(await _skip(game_id, session_id))

    return [(publish, json.dumps(msg)) for publish, msg in responses]


async def _join(
    game_id: uuid.UUID, session_id: str
) -> Tuple[ResponseType, ResponseType]:
    """Join an open game, start a full game, and return player and update responses."""
    player: Optional[int] = None
    publish_update: bool = False

    game: Dict[str, str] = await _load_game(game_id)
    bg: backgammon.Backgammon = _decode_game(game)

    async with session.load(session_id) as s:
        if s.game_id is None:
            if bg.match.game_state is backgammon.match.GameState.NOT_STARTED:
                p: int = 0 if "player_0" not in game else 1
                success: bool = await s.join_game(game_id, p)
                if success:
                    player = p
                    if player == 1:
                        bg.start()
                        await _update(game_id, bg)
                        publish_update = True
        elif s.id_ == game.get("player_0"):
            player = 0
        elif s.id_ == game.get("player_1"):
            player = 1

    player_resp: ResponseType = (
        False,
        {"code": ResponseCode.PLAYER.value, "player": player},
    )
    update_resp: ResponseType = (
        publish_update,
        {"code": ResponseCode.UPDATE.value, "game": bg.to_json()},
    )

    return player_resp, update_resp


@_authorized
async def _move(
    game_id: uuid.UUID,
    session_id: str,
    s: session.Session,
    game: Dict[str, str],
    bg: backgammon.Backgammon,
    move: List[Optional[int]],
) -> ResponseType:
    """Apply the move and return an update response."""
    try:
        bg.play(tuple(tuple(move[i : i + 2]) for i in range(0, len(move), 2)))
        bg.end_turn()
        return await _update(game_id, bg)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


@_authorized
async def _roll(
    game_id: uuid.UUID,
    session_id: str,
    s: session.Session,
    game: Dict[str, str],
    bg: backgammon.Backgammon,
) -> ResponseType:
    """Roll the dice and return an update response."""
    try:
        bg.roll()
        return await _update(game_id, bg)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


@_authorized
async def _skip(
    game_id: uuid.UUID,
    session_id: str,
    s: session.Session,
    game: Dict[str, str],
    bg: backgammon.Backgammon,
) -> ResponseType:
    """Skip the user's turn and return an update response."""
    try:
        bg.skip()
        return await _update(game_id, bg)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


async def _load_game(game_id: uuid.UUID) -> Dict[str, str]:
    """Load and return the game."""
    game: Dict[str, str]

    async with redis.get_connection() as conn:
        game = await conn.hgetall(f"game:{game_id}", encoding="utf-8")

    if not game:
        raise ValueError(f"Game not found: {game_id}")

    return game


def _decode_game(game: Dict[str, str]) -> backgammon.Backgammon:
    """Decode the position and match IDs and return a Backgammon instance."""
    return backgammon.Backgammon(game["position"], game["match"])


async def _update(game_id: uuid.UUID, bg: backgammon.Backgammon) -> ResponseType:
    """Update the stored game state and return an update response."""
    publish: bool = True

    async with redis.get_connection() as conn:
        pipeline: aioredis.commands.transaction.MultiExec = conn.multi_exec()
        pipeline.hset(f"game:{game_id}", "position", bg.position.encode())
        pipeline.hset(f"game:{game_id}", "match", bg.match.encode())
        await pipeline.execute()

    return publish, {"code": ResponseCode.UPDATE.value, "game": bg.to_json()}

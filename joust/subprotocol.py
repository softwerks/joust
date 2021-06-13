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

import enum
import functools
import json
import jsonschema
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import aioredis
import backgammon

from . import game
from . import redis
from . import session

logger: logging.Logger = logging.getLogger(__name__)

PayloadType = Dict[str, Any]
ResponseType = Tuple[bool, PayloadType]


@enum.unique
class Opcode(enum.Enum):
    ACCEPT: str = "accept"
    CONNECT: str = "connect"
    DISCONNECT: str = "disconnect"
    DOUBLE: str = "double"
    EXIT: str = "exit"
    MOVE: str = "move"
    REJECT: str = "reject"
    ROLL: str = "roll"
    SKIP: str = "skip"


@enum.unique
class ResponseCode(enum.Enum):
    CLOSE: str = "close"
    ERROR: str = "error"
    PLAYER: str = "player"
    STATUS: str = "status"
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
        game_id: str, session_token: str, *args, **kwargs
    ) -> Union[Callable, ResponseType]:
        async with session.load(session_token) as s:
            if s.game_id == game_id:
                g: game.Game = await game.load(game_id)

                if s.id_ == g.get_turn():
                    return await func(game_id, session_token, s, g, *args, **kwargs)

        return False, {"code": ResponseCode.ERROR.value, "error": "Unauthorized"}

    return wrapper


async def process_payload(
    game_id: str, session_token: str, serialized_payload: Union[str, bytes]
) -> List[Tuple[bool, str]]:
    """Process the payload and return a list of responses."""
    payload: PayloadType = _deserialize(serialized_payload)
    _validate(payload)
    return await _evaluate(game_id, session_token, payload)


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
    game_id: str, session_token: str, payload: PayloadType
) -> List[Tuple[bool, str]]:
    """Evalualuate the payload and return a list of JSON responses."""
    responses: List[ResponseType] = []

    opcode: Opcode = Opcode(payload["opcode"])

    if opcode is Opcode.ACCEPT:
        responses.append(await _accept(game_id, session_token))
    elif opcode is Opcode.CONNECT:
        responses.extend(await _connect(game_id, session_token))
    elif opcode is Opcode.DISCONNECT:
        disconnect_response: Optional[ResponseType] = await _disconnect(
            game_id, session_token
        )
        if disconnect_response is not None:
            responses.append(disconnect_response)
    elif opcode is Opcode.DOUBLE:
        responses.append(await _double(game_id, session_token))
    elif opcode is Opcode.EXIT:
        responses.extend(await _exit(game_id, session_token))
    elif opcode is Opcode.MOVE:
        try:
            responses.append(await _move(game_id, session_token, payload["move"]))
        except KeyError:
            raise ValueError("Missing move")
    elif opcode is Opcode.REJECT:
        responses.append(await _reject(game_id, session_token))
    elif opcode is Opcode.ROLL:
        responses.append(await _roll(game_id, session_token))
    elif opcode is Opcode.SKIP:
        responses.append(await _skip(game_id, session_token))

    return [(publish, json.dumps(msg)) for publish, msg in responses]


@_authorized
async def _accept(
    game_id: str,
    session_token: str,
    s: session.Session,
    g: game.Game,
) -> ResponseType:
    """Double and return an update response."""
    try:
        g.state.accept_double()
        return await _update(game_id, g.state)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


async def _disconnect(game_id: str, session_token: str) -> Optional[ResponseType]:
    """Update status and return a response."""
    g: game.Game = await game.load(game_id)

    async with session.load(session_token) as s:
        if await g.set_status(s.id_, game.Status("disconnected")):
            status_response: ResponseType = (
                True,
                {"code": ResponseCode.STATUS.value, "0": g.status_0, "1": g.status_1},
            )
            return status_response

    return None


async def _connect(game_id: str, session_token: str) -> Tuple[ResponseType, ...]:
    """Join an open game, start a full game, and return responses."""
    player: Optional[int] = None
    responses: Tuple[ResponseType, ...] = ()
    publish_update: bool = False

    g: game.Game = await game.load(game_id)

    async with session.load(session_token) as s:
        if s.game_id == game_id:
            player = await g.get_player(s.id_)
        elif (
            s.game_id is None
            and g.state.match.game_state is backgammon.match.GameState.NOT_STARTED
        ):
            player = await g.join_game(s.id_)
            if player is not None:
                await s.join_game(game_id)
            if player == 1:
                g.state.start()
                await _update(game_id, g.state)
                publish_update = True

        if player is not None:
            await g.set_status(s.id_, game.Status("connected"))
            player_response: ResponseType = (
                False,
                {"code": ResponseCode.PLAYER.value, "player": player},
            )
            status_response: ResponseType = (
                True,
                {"code": ResponseCode.STATUS.value, "0": g.status_0, "1": g.status_1},
            )
            responses += player_response, status_response

    update_response: ResponseType = (
        publish_update,
        {"code": ResponseCode.UPDATE.value, "id": g.state.encode()},
    )
    responses += (update_response,)

    return responses


@_authorized
async def _double(
    game_id: str,
    session_token: str,
    s: session.Session,
    g: game.Game,
) -> ResponseType:
    """Double and return an update response."""
    try:
        g.state.double()
        return await _update(game_id, g.state)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


async def _exit(game_id: str, session_token: str) -> Tuple[ResponseType, ...]:
    """Leave the game and return a response."""
    close_reponse: ResponseType = (False, {"code": ResponseCode.CLOSE.value})

    g: game.Game = await game.load(game_id)

    async with session.load(session_token) as s:
        if await g.set_status(s.id_, game.Status("forfeit")):
            await s.leave_game(game_id)
            status_reponse: ResponseType = (
                True,
                {"code": ResponseCode.STATUS.value, "0": g.status_0, "1": g.status_1},
            )
            return status_reponse, close_reponse

    return (close_reponse,)


@_authorized
async def _move(
    game_id: str,
    session_token: str,
    s: session.Session,
    g: game.Game,
    move: List[Optional[int]],
) -> ResponseType:
    """Apply the move and return an update response."""
    try:
        g.state.play(tuple(tuple(move[i : i + 2]) for i in range(0, len(move), 2)))
        return await _update(game_id, g.state)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


@_authorized
async def _reject(
    game_id: str,
    session_token: str,
    s: session.Session,
    g: game.Game,
) -> ResponseType:
    """Double and return an update response."""
    try:
        g.state.reject_double()
        return await _update(game_id, g.state)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


@_authorized
async def _roll(
    game_id: str,
    session_token: str,
    s: session.Session,
    g: game.Game,
) -> ResponseType:
    """Roll the dice and return an update response."""
    try:
        g.state.roll()
        return await _update(game_id, g.state)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


@_authorized
async def _skip(
    game_id: str,
    session_token: str,
    s: session.Session,
    g: game.Game,
) -> ResponseType:
    """Skip the user's turn and return an update response."""
    try:
        g.state.skip()
        return await _update(game_id, g.state)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


async def _update(game_id: str, bg: backgammon.Backgammon) -> ResponseType:
    """Update the stored game state and return an update response."""
    publish: bool = True

    async with redis.get_connection() as conn:
        pipeline: aioredis.commands.transaction.MultiExec = conn.multi_exec()
        pipeline.hset(f"game:{game_id}", "position", bg.position.encode())
        pipeline.hset(f"game:{game_id}", "match", bg.match.encode())
        await pipeline.execute()

    return publish, {"code": ResponseCode.UPDATE.value, "id": bg.encode()}

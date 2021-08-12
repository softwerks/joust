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

from datetime import datetime, timedelta, timezone
import enum
import functools
import json
import jsonschema
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import urllib.parse

import aioredis
import backgammon

from joust.game import channels
from joust.game import game
from joust.protocol import ServerProtocol
from joust import redis
from joust import session

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
    FEEDBACK: str = "feedback"
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


async def handler(websocket: ServerProtocol, path: str) -> None:
    try:
        url: urllib.parse.ParseResult = urllib.parse.urlparse(path)
        game_id: str = url.path.rsplit("/", 1)[-1]

        try:
            conn: aioredis.Redis = await redis.get_connection()
            if not await conn.exists(f"game:{game_id}"):
                raise ValueError

            async with channels.get_channel(websocket, game_id):
                async for message in websocket:
                    await _handle_message(websocket, game_id, message)

                await _handle_message(
                    websocket, game_id, json.dumps({"opcode": Opcode.DISCONNECT.value})
                )

        except ValueError:
            logger.warning(f"Invalid game ID: {game_id}")

    except IndexError:
        logger.warning(f"Invalid path: {path}")


async def _handle_message(
    websocket: ServerProtocol, game_id: str, message: str
) -> None:
    publish: bool

    try:
        responses: List[Tuple[bool, str]] = await _process_payload(
            game_id, websocket.session_token, message
        )
        for resp in responses:
            publish, msg = resp
            if publish:
                conn: aioredis.Redis = await redis.get_connection()
                await conn.publish(game_id, msg)
            else:
                await websocket.send(msg)
    except ValueError as error:
        logger.warning(f"{websocket.remote_address} - {game_id} [error]: {error}")


def _authorized(func: Callable) -> Callable:
    @functools.wraps(func)
    async def wrapper(
        game_id: str, session_token: str, *args, **kwargs
    ) -> Union[Callable, ResponseType]:
        user: session.Session = await session.load(session_token)
        if user.game_id == game_id:
            g: game.Game = await game.load(game_id)

            if user.id_ == g.get_turn():
                return await func(game_id, session_token, user, g, *args, **kwargs)

        return False, {"code": ResponseCode.ERROR.value, "error": "Unauthorized"}

    return wrapper


async def _process_payload(
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
        turn_owner: int = g.state.match.turn.value
        g.state.accept_double()
        g.swap_clock(turn_owner)
        return await _update(game_id, g)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


async def _disconnect(game_id: str, session_token: str) -> Optional[ResponseType]:
    """Update status and return a response."""
    g: game.Game = await game.load(game_id)

    user: session.Session = await session.load(session_token)
    if await g.set_status(user.id_, game.Status("disconnected")):
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

    user: session.Session = await session.load(session_token)

    if user.game_id == game_id:
        player = await g.get_player(user.id_)
    elif user.game_id is None and (g.player_0 == None or g.player_1 == None):
        player = await g.join_game(user.id_)
        if player is not None:
            await user.join_game(game_id)

    if player is not None:
        await g.set_status(user.id_, game.Status("connected"))
        player_response: ResponseType = (
            False,
            {"code": ResponseCode.PLAYER.value, "player": player},
        )
        status_response: ResponseType = (
            True,
            {"code": ResponseCode.STATUS.value, "0": g.status_0, "1": g.status_1},
        )
        responses += player_response, status_response

    if (
        g.state.match.game_state is backgammon.match.GameState.NOT_STARTED
        and g.player_0 != None
        and g.player_1 != None
    ):
        g.state.start()
        g.start_clock()
        publish_update = True

    update_response: ResponseType = await _update(game_id, g, publish_update)
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
        turn_owner: int = g.state.match.turn.value
        g.state.double()
        g.swap_clock(turn_owner)
        return await _update(game_id, g)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


async def _exit(game_id: str, session_token: str) -> Tuple[ResponseType, ...]:
    """Leave the game and return a response."""
    exit_reponse: ResponseType

    user: session.Session = await session.load(session_token)

    if user.feedback is None:
        exit_reponse = (False, {"code": ResponseCode.FEEDBACK.value})
    else:
        feedback: datetime = datetime.fromtimestamp(float(user.feedback), timezone.utc)
        now: datetime = datetime.now(timezone.utc)
        delta: timedelta = now - feedback

        if delta > timedelta(days=7):
            exit_reponse = (False, {"code": ResponseCode.FEEDBACK.value})
        else:
            exit_reponse = (False, {"code": ResponseCode.CLOSE.value})

    g: game.Game = await game.load(game_id)

    if await g.set_status(user.id_, game.Status("forfeit")):
        await user.leave_game(game_id)
        status_reponse: ResponseType = (
            True,
            {"code": ResponseCode.STATUS.value, "0": g.status_0, "1": g.status_1},
        )
        return status_reponse, exit_reponse

    return (exit_reponse,)


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
        turn_owner: int = g.state.match.turn.value
        g.state.play(tuple(tuple(move[i : i + 2]) for i in range(0, len(move), 2)))
        g.swap_clock(turn_owner)
        return await _update(game_id, g)
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
        turn_owner: int = g.state.match.turn.value
        g.state.reject_double()
        g.swap_clock(turn_owner)
        return await _update(game_id, g)
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
        return await _update(game_id, g)
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
        turn_owner: int = g.state.match.turn.value
        g.state.skip()
        g.swap_clock(turn_owner)
        return await _update(game_id, g)
    except backgammon.backgammon.BackgammonError as error:
        raise ValueError(error)


async def _update(game_id: str, g: game.Game, publish: bool = True) -> ResponseType:
    """Update the stored game state and return an update response."""
    conn: aioredis.Redis = await redis.get_connection()
    key: str = f"game:{game_id}"
    pipeline: aioredis.commands.transaction.MultiExec = conn.multi_exec()
    pipeline.hset(key, "position", g.state.position.encode())
    pipeline.hset(key, "match", g.state.match.encode())
    if g.time_0 is not None:
        pipeline.hset(key, "time_0", g.time_0)
    if g.time_1 is not None:
        pipeline.hset(key, "time_1", g.time_1)
    if g.timestamp is not None:
        pipeline.hset(key, "timestamp", g.timestamp)
    await pipeline.execute()

    return publish, {
        "code": ResponseCode.UPDATE.value,
        "id": g.state.encode(),
        "time0": g.time_0,
        "time1": g.time_1,
        "timestamp": g.timestamp,
    }

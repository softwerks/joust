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

import http
import logging
from typing import Mapping, Iterable, Optional, Tuple, Union
import urllib.parse

import websockets

from . import redis

logger: logging.Logger = logging.getLogger(__name__)


class ServerProtocol(websockets.WebSocketServerProtocol):
    game_id: str
    auth_token: str
    session_token: str

    async def process_request(
        self, path: str, request_headers: websockets.http.Headers
    ) -> Optional[
        Tuple[
            http.HTTPStatus,
            Union[
                websockets.http.Headers, Mapping[str, str], Iterable[Tuple[str, str]]
            ],
            bytes,
        ]
    ]:
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse(path)
        query_params: dict = urllib.parse.parse_qs(parsed_url.query)

        try:
            self.game_id = parsed_url.path.rsplit("/", 1)[-1]
        except ValueError:
            logger.info(f"Invalid or missing game ID: {parsed_url.path}")
            return (http.HTTPStatus.BAD_REQUEST, [], b"")

        try:
            self.auth_token = query_params["token"][0]
        except KeyError:
            logger.info(f"Missing credentials: {query_params}")
            return (
                http.HTTPStatus.UNAUTHORIZED,
                [("WWW-Authenticate", "Token")],
                b"Missing credentials\n",
            )

        async with redis.get_connection() as conn:
            session_token: Optional[str] = await conn.get(
                f"websocket:{self.auth_token}", encoding="utf-8"
            )
        if session_token is None:
            logger.info(f"Invalid token: {self.auth_token}")
            return (
                http.HTTPStatus.UNAUTHORIZED,
                [("WWW-Authenticate", "Token")],
                b"Invalid credentials\n",
            )
        self.session_token = session_token

        return await super().process_request(path, request_headers)

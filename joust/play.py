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

from typing import Any
import uuid

import starlette.endpoints
import starlette.websockets


class GameEndpoint(starlette.endpoints.WebSocketEndpoint):
    encoding: str = "text"

    async def on_connect(self, websocket: starlette.websockets.WebSocket):
        print(websocket.headers)
        await websocket.accept()

    async def on_receive(
        self, websocket: starlette.websockets.WebSocket, data: Any
    ) -> None:
        game_id: uuid.UUID = websocket.path_params["game_id"]
        print(game_id)
        await websocket.send_text(f"Message text was: {data}")

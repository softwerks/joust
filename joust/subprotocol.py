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

logger: logging.Logger = logging.getLogger(__name__)

payload_schema: Dict[str, Any] = {
    "type": "object",
    "properties": {"opcode": {"type": "integer"}, "data": {"type": "object"}},
    "required": ["opcode"],
}


class Opcode(enum.Enum):
    STUB: int = 1


async def process_payload(serialized_payload: Union[str, bytes]) -> str:
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

    def evaluate(deserialized_payload: Dict[str, Any]) -> str:
        return "stub"

    deserialized_payload: Dict[str, Any] = deserialize(serialized_payload)
    validate(deserialized_payload)
    return evaluate(deserialized_payload)

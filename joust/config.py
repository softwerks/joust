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

import argparse
import logging
from typing import Tuple

parser: argparse.ArgumentParser = argparse.ArgumentParser(
    description="Joust Websocket Server"
)

levels: Tuple[str, ...] = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
parser.add_argument("-l", "--log-level", default="INFO", type=str, choices=levels)
parser.add_argument("-p", "--port", default=5555, type=int, help="port number")

args: argparse.Namespace = parser.parse_args()

logging.basicConfig(level=args.log_level)

PORT: int = args.port

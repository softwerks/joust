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

import asyncio
import logging

try:
    import hupper
except ImportError:
    hupper = None

from . import config
from . import server

logger: logging.Logger = logging.getLogger(__name__)


async def _serve() -> None:
    async with server.Server() as serv:
        await serv.serve()


def main() -> None:
    if config.RELOAD:
        if hupper is None:
            logger.error(
                "reload requires hupper - install with 'pip install -e .[dev]'"
            )
        else:
            reloader: hupper.interfaces.IReloaderProxy = hupper.start_reloader(
                "joust.__main__.main"
            )
    asyncio.run(_serve())


if __name__ == "__main__":
    main()

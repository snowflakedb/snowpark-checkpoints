# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional

from snowflake.snowpark_checkpoints_configuration.io_utils import (
    EnvStrategy,
    IODefaultStrategy,
)
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton


class IOFileManager(metaclass=Singleton):
    def __init__(self, strategy: Optional[EnvStrategy] = None):
        self.strategy = strategy or IODefaultStrategy()

    def file_exists(self, path: str) -> bool:
        return self.strategy.file_exists(path)

    def read(
        self, file_path: str, mode: str = "r", encoding: Optional[str] = None
    ) -> str:
        return self.strategy.read(file_path, mode, encoding)

    def getcwd(self) -> str:
        return self.strategy.getcwd()

    def set_strategy(self, strategy: EnvStrategy):
        """Set the strategy for file and directory operations.

        Args:
            strategy (EnvStrategy): The strategy to use for file and directory operations.

        """
        self.strategy = strategy


def get_io_file_manager():
    """Get the singleton instance of IOFileManager.

    Returns:
        IOFileManager: The singleton instance of IOFileManager.

    """
    return IOFileManager()

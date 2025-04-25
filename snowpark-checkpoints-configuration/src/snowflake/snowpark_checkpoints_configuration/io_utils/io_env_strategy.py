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

from abc import ABC, abstractmethod
from typing import Optional


class EnvStrategy(ABC):

    """An abstract base class that defines methods for file and directory operations.

    Subclasses should implement these methods to provide environment-specific behavior.
    """

    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """Check if a file exists.

        Args:
            path: The path to the file.

        Returns:
            bool: True if the file exists, False otherwise.

        """

    @abstractmethod
    def read(
        self, file_path: str, mode: str = "r", encoding: Optional[str] = None
    ) -> str:
        """Read content from a file.

        Args:
            file_path: The path to the file to read from.
            mode: The mode in which to open the file.
            encoding: The encoding to use for reading the file.

        Returns:
            str: The content of the file.

        """

    @abstractmethod
    def getcwd(self) -> str:
        """Get the current working directory.

        Returns:
            str: The current working directory.

        """

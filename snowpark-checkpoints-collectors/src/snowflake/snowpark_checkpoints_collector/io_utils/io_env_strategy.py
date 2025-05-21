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
from pathlib import Path
from typing import Optional


class EnvStrategy(ABC):

    """An abstract base class that defines methods for file and directory operations.

    Subclasses should implement these methods to provide environment-specific behavior.
    """

    @abstractmethod
    def mkdir(self, path: str, exist_ok: bool = False) -> None:
        """Create a directory.

        Args:
            path: The name of the directory to create.
            exist_ok: If False, an error is raised if the directory already exists.

        """

    @abstractmethod
    def folder_exists(self, path: str) -> bool:
        """Check if a folder exists.

        Args:
            path: The path to the folder.

        Returns:
            bool: True if the folder exists, False otherwise.

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
    def write(self, file_path: str, file_content: str, overwrite: bool = True) -> None:
        """Write content to a file.

        Args:
            file_path: The name of the file to write to.
            file_content: The content to write to the file.
            overwrite: If True, overwrite the file if it exists.

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
    def read_bytes(self, file_path: str) -> bytes:
        """Read binary content from a file.

        Args:
            file_path: The path to the file to read from.

        Returns:
            bytes: The binary content of the file.

        """

    @abstractmethod
    def ls(self, path: str, recursive: bool = False) -> list[str]:
        """List the contents of a directory.

        Args:
            path: The path to the directory.
            recursive: If True, list the contents recursively.

        Returns:
            list[str]: A list of the contents of the directory.

        """

    @abstractmethod
    def getcwd(self) -> str:
        """Get the current working directory.

        Returns:
            str: The current working directory.

        """

    @abstractmethod
    def remove_dir(self, path: str) -> None:
        """Remove a directory and all its contents.

        Args:
            path: The path to the directory to remove.

        """

    @abstractmethod
    def telemetry_path_files(self, path: str) -> Path:
        """Get the path to the telemetry files.

        Args:
            path: The path to the telemetry directory.

        Returns:
            Path: The path object representing the telemetry files.

        """

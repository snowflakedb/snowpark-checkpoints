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

import glob
import os

from pathlib import Path
from typing import Optional

from snowflake.snowpark_checkpoints.io_utils import EnvStrategy


class IODefaultStrategy(EnvStrategy):
    def mkdir(self, path: str, exist_ok: bool = False) -> None:
        os.makedirs(path, exist_ok=exist_ok)

    def folder_exists(self, path: str) -> bool:
        return os.path.isdir(path)

    def file_exists(self, path: str) -> bool:
        return os.path.isfile(path)

    def write(self, file_path: str, file_content: str, overwrite: bool = True) -> None:
        mode = "w" if overwrite else "x"
        with open(file_path, mode) as file:
            file.write(file_content)

    def read(
        self, file_path: str, mode: str = "r", encoding: Optional[str] = None
    ) -> str:
        with open(file_path, mode=mode, encoding=encoding) as file:
            return file.read()

    def read_bytes(self, file_path: str) -> bytes:
        with open(file_path, mode="rb") as f:
            return f.read()

    def ls(self, path: str, recursive: bool = False) -> list[str]:
        return glob.glob(path, recursive=recursive)

    def getcwd(self) -> str:
        return os.getcwd()

    def telemetry_path_files(self, path: str) -> Path:
        return Path(path)

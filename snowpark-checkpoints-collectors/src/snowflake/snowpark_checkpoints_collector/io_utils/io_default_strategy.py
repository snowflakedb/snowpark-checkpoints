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

from typing import Optional

from snowflake.snowpark_checkpoints_collector.io_utils import EnvStrategy


class IODefaultStrategy(EnvStrategy):
    def mkdir(self, path: str, exist_ok=False) -> bool:
        try:
            os.makedirs(path, exist_ok=exist_ok)
            return True
        except Exception:
            return False

    def folder_exists(self, path: str) -> bool:
        try:
            return os.path.isdir(path)
        except Exception:
            return False

    def file_exists(self, path: str) -> bool:
        try:
            return os.path.isfile(path)
        except Exception:
            return False

    def write(self, file_path: str, file_content: str, overwrite: bool = True) -> bool:
        try:
            mode = "w" if overwrite else "x"
            with open(file_path, mode) as file:
                file.write(file_content)
            return True
        except Exception:
            return False

    def read(
        self, file_path: str, mode: str = "r", encoding: str = None
    ) -> Optional[str]:
        try:
            with open(file_path, mode=mode, encoding=encoding) as file:
                return file.read()
        except Exception:
            return None

    def read_bytes(self, file_path: str) -> Optional[bytes]:
        try:
            with open(file_path, mode="rb") as f:
                return f.read()
        except Exception:
            return None

    def ls(self, path: str, recursive: bool = False) -> list[str]:
        try:
            return glob.glob(path, recursive=recursive)
        except Exception:
            return []

    def getcwd(self) -> str:
        try:
            return os.getcwd()
        except Exception:
            return ""

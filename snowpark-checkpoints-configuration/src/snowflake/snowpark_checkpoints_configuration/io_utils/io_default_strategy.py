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

import os

from typing import Optional

from snowflake.snowpark_checkpoints_configuration.io_utils import EnvStrategy


class IODefaultStrategy(EnvStrategy):
    def file_exists(self, path: str) -> bool:
        return os.path.isfile(path)

    def read(
        self, file_path: str, mode: str = "r", encoding: Optional[str] = None
    ) -> str:
        with open(file_path, mode=mode, encoding=encoding) as file:
            return file.read()

    def getcwd(self) -> str:
        return os.getcwd()

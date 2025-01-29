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
from datetime import datetime
from enum import Enum

from snowflake.snowpark_checkpoints_collector.utils import file_utils


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

TIMESTAMP_KEY = "timestamp"
FILE_KEY = "file"
RESULT_KEY = "result"
LINE_OF_CODE_KEY = "line_of_code"
CHECKPOINT_NAME_KEY = "checkpoint_name"


class CollectionResult(Enum):
    FAIL = "FAIL"
    PASS = "PASS"


class CollectionPointResult:

    """Class for checkpoint collection results.

    Attributes:
        _timestamp (timestamp): the timestamp when collection started.
        _file_path (str): the full path where checkpoint is.
        _line_of_code (int): the line of code where the checkpoint is.
        _checkpoint_name (str): the checkpoint name.
        _result (CollectionResult): the result status of the checkpoint collection point.

    """

    def __init__(
        self,
        file_path: str,
        line_of_code: int,
        checkpoint_name: str,
    ) -> None:
        """Init CollectionPointResult.

        Args:
            file_path (str): the full path where checkpoint is.
            line_of_code (int): the line of code where the checkpoint is.
            checkpoint_name (str): the checkpoint name.

        """
        self._timestamp = datetime.now()
        self._file_path = file_path
        self._line_of_code = line_of_code
        self._checkpoint_name = checkpoint_name
        self._result = None

    @property
    def result(self):
        """Get the result status of the checkpoint collection point."""
        return self._result

    @result.setter
    def result(self, value):
        """Set the result status of the checkpoint collection point."""
        self._result = value

    def get_collection_result_data(self) -> dict[str, any]:
        """Get the results of the checkpoint collection point."""
        timestamp_with_format = self._timestamp.strftime(TIMESTAMP_FORMAT)
        relative_path = file_utils.get_relative_file_path(self._file_path)

        collection_point_result = {
            TIMESTAMP_KEY: timestamp_with_format,
            FILE_KEY: relative_path,
            LINE_OF_CODE_KEY: self._line_of_code,
            CHECKPOINT_NAME_KEY: self._checkpoint_name,
            RESULT_KEY: self.result.value,
        }

        return collection_point_result

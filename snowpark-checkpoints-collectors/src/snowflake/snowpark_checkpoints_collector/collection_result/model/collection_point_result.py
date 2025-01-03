#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
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
        timestamp (timestamp): the timestamp when collection started.
        file_path (str): the full path where checkpoint is.
        line_of_code (int): the line of code where the checkpoint is.
        checkpoint_name (str): the checkpoint name.

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
        self.timestamp = datetime.now()
        self.file_path = file_path
        self.line_of_code = line_of_code
        self.checkpoint_name = checkpoint_name
        self.result = None

    def set_collection_point_result_to_pass(self) -> None:
        """Set the result status of the checkpoint to pass."""
        self.result = CollectionResult.PASS

    def set_collection_point_result_to_fail(self) -> None:
        """Set the result status of the checkpoint to fail."""
        self.result = CollectionResult.FAIL

    def get_collection_result_data(self) -> dict[str, any]:
        """Get the results of the checkpoint."""
        timestamp_with_format = self.timestamp.strftime(TIMESTAMP_FORMAT)
        relative_path = file_utils.get_relative_file_path(self.file_path)

        collection_point_result = {
            TIMESTAMP_KEY: timestamp_with_format,
            FILE_KEY: relative_path,
            LINE_OF_CODE_KEY: self.line_of_code,
            CHECKPOINT_NAME_KEY: self.checkpoint_name,
            RESULT_KEY: self.result.value,
        }

        return collection_point_result

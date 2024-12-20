from datetime import datetime
from enum import Enum

from pyspark.sql.types import StructType


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

TIMESTAMP_KEY = "timestamp"
SCHEMA_DF_KEY = "schema_df"
RESULT_KEY = "result"
LINE_OF_CODE_KEY = "line_of_code"


class CollectionResult(Enum):
    FAIL = "FAIL"
    PASS = "PASS"


class CollectionPointResult:

    """Class for checkpoint collection results.

    Attributes:
        file_path (str): the full path where checkpoint is.
        checkpoint_name (str): the checkpoint name.
        schema_df (pyspark.sql.types.StructType): the schema of DataFrame to collect.
        line_of_code (int): the line of code where the checkpoint is.

    """

    def __init__(
        self,
        file_path: str,
        checkpoint_name: str,
        schema_df: StructType,
        line_of_code: int,
    ) -> None:
        """Init BinaryColumnCollector.

        Args:
            file_path (str): the full path where checkpoint is.
            checkpoint_name (str): the checkpoint name.
            schema_df (pyspark.sql.types.StructType): the schema of DataFrame to collect.
            line_of_code (int): the line of code where the checkpoint is.

        """
        self.file_path = file_path
        self.checkpoint_name = checkpoint_name
        self.timestamp = datetime.now()
        self.schema_df = schema_df
        self.result = None
        self.line_of_code = line_of_code

    def set_collection_point_result_to_pass(self) -> None:
        """Set the result status of the checkpoint to pass."""
        self.result = CollectionResult.PASS

    def set_collection_point_result_to_fail(self) -> None:
        """Set the result status of the checkpoint to fail."""
        self.result = CollectionResult.FAIL

    def get_collection_result_data(self) -> dict[str, any]:
        """Get the results of the checkpoint."""
        timestamp_with_format = self.timestamp.strftime(TIMESTAMP_FORMAT)
        schema_df_dict = self.schema_df.jsonValue()

        collection_point_result = {
            TIMESTAMP_KEY: timestamp_with_format,
            SCHEMA_DF_KEY: schema_df_dict,
            RESULT_KEY: self.result.value,
            LINE_OF_CODE_KEY: self.line_of_code,
        }

        return collection_point_result

from datetime import datetime
from enum import Enum

from pyspark.sql.types import StructType

from snowflake.snowpark_checkpoints_collector.utils import file_utils


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

TIMESTAMP_KEY = "timestamp"
SCHEMA_DF_KEY = "schema_df"
RESULT_KEY = "result"
LOCATION_KEY = "location"
FILE_KEY = "file"
FUNCTION_KEY = "function"
TYPE_KEY = "type"


class CollectionResult(Enum):
    FAIL = "FAIL"
    PASS = "PASS"


class CollectionPointResult:
    def __init__(
        self,
        file_path: str,
        checkpoint_name: str,
        schema_df: StructType,
        location: int,
        df_name: str,
    ) -> None:
        self.file_path = file_path
        self.checkpoint_name = checkpoint_name
        self.timestamp = datetime.now()
        self.schema_df = schema_df
        self.result = None
        self.location = location
        self.df_name = df_name
        self.type = "collection"

    def set_collection_point_result_to_pass(self) -> None:
        self.result = CollectionResult.PASS

    def set_collection_point_result_to_fail(self) -> None:
        self.result = CollectionResult.FAIL

    def get_collection_result_data(self) -> dict[str, any]:
        timestamp_with_format = self.timestamp.strftime(TIMESTAMP_FORMAT)
        schema_df_dict = self.schema_df.jsonValue()
        relative_file_path = file_utils.get_relative_file_path(self.file_path)

        collection_point_result = {
            TIMESTAMP_KEY: timestamp_with_format,
            SCHEMA_DF_KEY: schema_df_dict,
            RESULT_KEY: self.result.value,
            LOCATION_KEY: self.location,
            FILE_KEY: relative_file_path,
            FUNCTION_KEY: self.df_name,
            TYPE_KEY: self.type,
        }

        return collection_point_result

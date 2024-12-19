from datetime import datetime
from enum import Enum

from pyspark.sql.types import StructType


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

TIMESTAMP_KEY = "timestamp"
SCHEMA_DF_KEY = "schema_df"
RESULT_KEY = "result"
LINES_OF_CODE_KEY = "line_of_code"


class CollectionResult(Enum):
    FAIL = "FAIL"
    PASS = "PASS"


class CollectionPointResult:
    def __init__(
        self,
        file_path: str,
        checkpoint_name: str,
        schema_df: StructType,
        line_of_code: int,
    ) -> None:
        self.file_path = file_path
        self.checkpoint_name = checkpoint_name
        self.timestamp = datetime.now()
        self.schema_df = schema_df
        self.result = None
        self.line_of_code = line_of_code

    def set_collection_point_result_to_pass(self) -> None:
        self.result = CollectionResult.PASS

    def set_collection_point_result_to_fail(self) -> None:
        self.result = CollectionResult.FAIL

    def get_collection_result_data(self) -> dict[str, any]:
        timestamp_with_format = self.timestamp.strftime(TIMESTAMP_FORMAT)
        schema_df_dict = self.schema_df.jsonValue()

        collection_point_result = {
            TIMESTAMP_KEY: timestamp_with_format,
            SCHEMA_DF_KEY: schema_df_dict,
            RESULT_KEY: self.result.value,
            LINES_OF_CODE_KEY: self.line_of_code,
        }

        return collection_point_result

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

# Skip type
from enum import IntEnum
from typing import Final


class CheckpointMode(IntEnum):

    """Enum class representing the validation mode."""

    SCHEMA = 1
    """Validate against a schema file"""
    DATAFRAME = 2
    """Validate against a dataframe"""


# Execution mode
SCHEMA_EXECUTION_MODE: Final[str] = "Schema"
DATAFRAME_EXECUTION_MODE: Final[str] = "Dataframe"


# File position on stack
STACK_POSITION_CHECKPOINT: Final[int] = 6

# Validation status
PASS_STATUS: Final[str] = "PASS"
FAIL_STATUS: Final[str] = "FAIL"

# Validation result keys
DEFAULT_KEY: Final[str] = "default"


# Skip type
SKIP_ALL: Final[str] = "skip_all"

# Supported types
BOOLEAN_TYPE: Final[str] = "boolean"
BINARY_TYPE: Final[str] = "binary"
BYTE_TYPE: Final[str] = "byte"
CHAR_TYPE: Final[str] = "char"
DATE_TYPE: Final[str] = "date"
DAYTIMEINTERVAL_TYPE: Final[str] = "daytimeinterval"
DECIMAL_TYPE: Final[str] = "decimal"
DOUBLE_TYPE: Final[str] = "double"
FLOAT_TYPE: Final[str] = "float"
INTEGER_TYPE: Final[str] = "integer"
LONG_TYPE: Final[str] = "long"
SHORT_TYPE: Final[str] = "short"
STRING_TYPE: Final[str] = "string"
TIMESTAMP_TYPE: Final[str] = "timestamp"
TIMESTAMP_NTZ_TYPE: Final[str] = "timestamp_ntz"
VARCHAR_TYPE: Final[str] = "varchar"

# Pandas data types
PANDAS_BOOLEAN_DTYPE: Final[str] = "bool"
PANDAS_DATETIME_DTYPE: Final[str] = "datetime64[ns]"
PANDAS_FLOAT_DTYPE: Final[str] = "float64"
PANDAS_INTEGER_DTYPE: Final[str] = "int64"
PANDAS_OBJECT_DTYPE: Final[str] = "object"
PANDAS_TIMEDELTA_DTYPE: Final[str] = "timedelta64[ns]"

# Schemas keys
COLUMNS_KEY: Final[str] = "columns"
COUNT_KEY: Final[str] = "rows_count"
DECIMAL_PRECISION_KEY: Final[str] = "decimal_precision"
FALSE_COUNT_KEY: Final[str] = "false_count"
FORMAT_KEY: Final[str] = "format"
NAME_KEY: Final[str] = "name"
MARGIN_ERROR_KEY: Final[str] = "margin_error"
MAX_KEY: Final[str] = "max"
MEAN_KEY: Final[str] = "mean"
MIN_KEY: Final[str] = "min"
NULL_COUNT_KEY: Final[str] = "rows_null_count"
NULLABLE_KEY: Final[str] = "nullable"
ROWS_NOT_NULL_COUNT_KEY: Final[str] = "rows_not_null_count"
TRUE_COUNT_KEY: Final[str] = "true_count"
TYPE_KEY: Final[str] = "type"
ROWS_COUNT_KEY: Final[str] = "rows_count"
FORMAT_KEY: Final[str] = "format"

DATAFRAME_CUSTOM_DATA_KEY: Final[str] = "custom_data"
DATAFRAME_PANDERA_SCHEMA_KEY: Final[str] = "pandera_schema"

# Default values
DEFAULT_DATE_FORMAT: Final[str] = "%Y-%m-%d"

# SQL Column names
TABLE_NAME_COL: Final[str] = "TABLE_NAME"
CREATED_COL: Final[str] = "CREATED"

# SQL Table names
INFORMATION_SCHEMA_TABLE_NAME: Final[str] = "INFORMATION_SCHEMA"
TABLES_TABLE_NAME: Final[str] = "TABLES"

# SQL Query
EXCEPT_HASH_AGG_QUERY: Final[
    str
] = "SELECT HASH_AGG(*) FROM IDENTIFIER(:1) EXCEPT SELECT HASH_AGG(*) FROM IDENTIFIER(:2)"

# Table checkpoints name
CHECKPOINT_TABLE_NAME_FORMAT: Final[str] = "{}_snowpark"

# Write mode
OVERWRITE_MODE: Final[str] = "overwrite"

# Validation modes
VALIDATION_MODE_KEY: Final[str] = "validation_mode"
PIPELINES_KEY: Final[str] = "pipelines"

# File name
CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME: Final[str] = "{}.json"
CHECKPOINTS_JSON_FILE_NAME: Final[str] = "checkpoints.json"
SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME: Final[str] = "snowpark-checkpoints-output"
CHECKPOINT_PARQUET_OUTPUT_FILE_FORMAT_NAME: Final[str] = "{}.parquet"
VALIDATION_RESULTS_JSON_FILE_NAME: Final[str] = "checkpoint_validation_results.json"

# Environment variables
SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR: Final[
    str
] = "SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH"

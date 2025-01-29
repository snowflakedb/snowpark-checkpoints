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

import locale

from enum import IntEnum


class CheckpointMode(IntEnum):

    """Enum class representing the collection mode."""

    SCHEMA = 1
    """Collect automatic schema inference"""
    DATAFRAME = 2
    """Export DataFrame as Parquet file to Snowflake"""


# CONSTANTS
ARRAY_COLUMN_TYPE = "array"
BINARY_COLUMN_TYPE = "binary"
BOOLEAN_COLUMN_TYPE = "boolean"
BYTE_COLUMN_TYPE = "byte"
DATE_COLUMN_TYPE = "date"
DAYTIMEINTERVAL_COLUMN_TYPE = "daytimeinterval"
DECIMAL_COLUMN_TYPE = "decimal"
DOUBLE_COLUMN_TYPE = "double"
FLOAT_COLUMN_TYPE = "float"
INTEGER_COLUMN_TYPE = "integer"
LONG_COLUMN_TYPE = "long"
MAP_COLUMN_TYPE = "map"
NULL_COLUMN_TYPE = "void"
SHORT_COLUMN_TYPE = "short"
STRING_COLUMN_TYPE = "string"
STRUCT_COLUMN_TYPE = "struct"
TIMESTAMP_COLUMN_TYPE = "timestamp"
TIMESTAMP_NTZ_COLUMN_TYPE = "timestamp_ntz"

PANDAS_BOOLEAN_DTYPE = "bool"
PANDAS_DATETIME_DTYPE = "datetime64[ns]"
PANDAS_FLOAT_DTYPE = "float64"
PANDAS_INTEGER_DTYPE = "int64"
PANDAS_OBJECT_DTYPE = "object"
PANDAS_TIMEDELTA_DTYPE = "timedelta64[ns]"

NUMERIC_TYPE_COLLECTION = [
    BYTE_COLUMN_TYPE,
    DOUBLE_COLUMN_TYPE,
    FLOAT_COLUMN_TYPE,
    INTEGER_COLUMN_TYPE,
    LONG_COLUMN_TYPE,
    SHORT_COLUMN_TYPE,
]

INTEGER_TYPE_COLLECTION = [
    BYTE_COLUMN_TYPE,
    INTEGER_COLUMN_TYPE,
    LONG_COLUMN_TYPE,
    SHORT_COLUMN_TYPE,
]

PANDAS_OBJECT_TYPE_COLLECTION = [
    STRING_COLUMN_TYPE,
    ARRAY_COLUMN_TYPE,
    MAP_COLUMN_TYPE,
    NULL_COLUMN_TYPE,
    STRUCT_COLUMN_TYPE,
]

BETWEEN_CHECK_ERROR_MESSAGE_FORMAT = "Value must be between {} and {}"

# SCHEMA CONTRACT KEYS CONSTANTS
COLUMN_ALLOW_NULL_KEY = "allow_null"
COLUMN_COUNT_KEY = "rows_count"
COLUMN_DECIMAL_PRECISION_KEY = "decimal_precision"
COLUMN_FALSE_COUNT_KEY = "false_count"
COLUMN_FORMAT_KEY = "format"
COLUMN_IS_NULLABLE_KEY = "nullable"
COLUMN_IS_UNIQUE_SIZE_KEY = "is_unique_size"
COLUMN_KEY_TYPE_KEY = "key_type"
COLUMN_MARGIN_ERROR_KEY = "margin_error"
COLUMN_MAX_KEY = "max"
COLUMN_MAX_LENGTH_KEY = "max_length"
COLUMN_MAX_SIZE_KEY = "max_size"
COLUMN_MEAN_KEY = "mean"
COLUMN_MEAN_SIZE_KEY = "mean_size"
COLUMN_METADATA_KEY = "metadata"
COLUMN_MIN_KEY = "min"
COLUMN_MIN_LENGTH_KEY = "min_length"
COLUMN_MIN_SIZE_KEY = "min_size"
COLUMN_NAME_KEY = "name"
COLUMN_NULL_COUNT_KEY = "null_count"
COLUMN_NULL_VALUE_PROPORTION_KEY = "null_value_proportion"
COLUMN_ROWS_NOT_NULL_COUNT_KEY = "rows_not_null_count"
COLUMN_ROWS_NULL_COUNT_KEY = "rows_null_count"
COLUMN_SIZE_KEY = "size"
COLUMN_TRUE_COUNT_KEY = "true_count"
COLUMN_TYPE_KEY = "type"
COLUMN_VALUE_KEY = "value"
COLUMN_VALUE_TYPE_KEY = "value_type"
COLUMNS_KEY = "columns"

DATAFRAME_CUSTOM_DATA_KEY = "custom_data"
DATAFRAME_PANDERA_SCHEMA_KEY = "pandera_schema"

PANDERA_COLUMN_TYPE_KEY = "dtype"

CONTAINS_NULL_KEY = "containsNull"
ELEMENT_TYPE_KEY = "elementType"
FIELD_METADATA_KEY = "metadata"
FIELDS_KEY = "fields"
KEY_TYPE_KEY = "keyType"
NAME_KEY = "name"
VALUE_CONTAINS_NULL_KEY = "valueContainsNull"
VALUE_TYPE_KEY = "valueType"

# DIRECTORY AND FILE NAME CONSTANTS
SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME = "snowpark-checkpoints-output"
CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT = "{}.json"
CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT = "{}.parquet"
COLLECTION_RESULT_FILE_NAME = "checkpoint_collection_results.json"

# MISC KEYS
DECIMAL_TOKEN_KEY = "decimal_point"
DOT_PARQUET_EXTENSION = ".parquet"
DOT_IPYNB_EXTENSION = ".ipynb"
UNKNOWN_SOURCE_FILE = "unknown"
UNKNOWN_LINE_OF_CODE = -1
BACKSLASH_TOKEN = "\\"
SLASH_TOKEN = "/"
PYSPARK_NONE_SIZE_VALUE = -1
PANDAS_LONG_TYPE = "Int64"

# ENVIRONMENT VARIABLES
SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR = (
    "SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH"
)


def get_decimal_token() -> str:
    """Return the decimal token based on the local environment.

    Returns:
        str: The decimal token.

    """
    decimal_token = locale.localeconv()[DECIMAL_TOKEN_KEY]
    return decimal_token

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import locale

from enum import IntEnum


class CheckpointMode(IntEnum):
    SCHEMA = 1
    DATAFRAME = 2


# CONSTANTS
BOOLEAN_COLUMN_TYPE = "boolean"
BYTE_COLUMN_TYPE = "byte"
DATE_COLUMN_TYPE = "date"
DAYTIMEINTERVAL_COLUMN_TYPE = "daytimeinterval"
DECIMAL_COLUMN_TYPE = "decimal"
DOUBLE_COLUMN_TYPE = "double"
FLOAT_COLUMN_TYPE = "float"
INTEGER_COLUMN_TYPE = "integer"
LONG_COLUMN_TYPE = "long"
SHORT_COLUMN_TYPE = "short"
STRING_COLUMN_TYPE = "string"
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

BETWEEN_CHECK_ERROR_MESSAGE_FORMAT = "Value must be between {} and {}"

# SCHEMA CONTRACT KEYS CONSTANTS
COLUMNS_KEY = "columns"
COLUMN_COUNT_KEY = "rows_count"
COLUMN_DECIMAL_PRECISION_KEY = "decimal_precision"
COLUMN_FALSE_COUNT_KEY = "false_count"
COLUMN_FORMAT_KEY = "format"
COLUMN_MARGIN_ERROR_KEY = "margin_error"
COLUMN_MAX_KEY = "max"
COLUMN_MEAN_KEY = "mean"
COLUMN_MIN_KEY = "min"
COLUMN_NAME_KEY = "name"
COLUMN_NULL_COUNT_KEY = "rows_null_count"
COLUMN_ROWS_NOT_NULL_COUNT_KEY = "rows_not_null_count"
COLUMN_TRUE_COUNT_KEY = "true_count"
COLUMN_TYPE_KEY = "type"

DATAFRAME_CUSTOM_DATA_KEY = "custom_data"
DATAFRAME_PANDERA_SCHEMA_KEY = "pandera_schema"

PANDERA_COLUMN_TYPE_KEY = "dtype"

# DIRECTORY AND FILE NAME CONSTANTS
SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME = "snowpark-checkpoints-output"
CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT = "{}.json"
CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT = "{}.parquet"

# MISC KEYS
DECIMAL_TOKEN_KEY = "decimal_point"
DOT_PARQUET_EXTENSION = ".parquet"
BACKSLASH_TOKEN = "\\"
SLASH_TOKEN = "/"


def get_decimal_token() -> str:
    """Return the decimal token based on the local environment.

    Returns:
        str: The decimal token.

    """
    decimal_token = locale.localeconv()[DECIMAL_TOKEN_KEY]
    return decimal_token

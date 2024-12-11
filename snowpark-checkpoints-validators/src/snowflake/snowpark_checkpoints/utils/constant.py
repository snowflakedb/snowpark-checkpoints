#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Skip type
from typing import Final


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
ROWS_NOT_NULL_COUNT_KEY: Final[str] = "rows_not_null_count"
TRUE_COUNT_KEY: Final[str] = "true_count"
TYPE_KEY: Final[str] = "type"

DATAFRAME_CUSTOM_DATA_KEY: Final[str] = "custom_data"
DATAFRAME_PANDERA_SCHEMA_KEY: Final[str] = "pandera_schema"

# File name
CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME: Final[str] = "{}.json"
SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_FORMAT_NAME: Final[
    str
] = "snowpark-checkpoints-output"

# Error messages
SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR: Final[
    str
] = "Snowpark output schema validation error"
COLUMN_NOT_FOUND_FORMAT_ERROR: Final[str] = "Column {} not found in schema"
BETWEEN_CHECK_ERROR_MESSAGE_FORMAT_ERROR: Final[str] = "Value must be between {} and {}"
PANDERA_NOT_FOUND_JSON_FORMAT_ERROR: Final[
    str
] = "Pandera schema not found in the JSON file for checkpoint: {}"
COLUMNS_NOT_FOUND_JSON_FORMAT_ERROR: Final[
    str
] = "Columns not found in the JSON file for checkpoint: {}"
DATA_FRAME_IS_REQUIRED_ERROR: Final[str] = "DataFrame is required"
CHECKPOINT_NAME_IS_REQUIRED_ERROR: Final[str] = "Checkpoint name is required"
CHECKPOINT_JSON_OUTPUT_DIRECTORY_ERROR: Final[
    str
] = "Output directory snowpark-checkpoints-output does not exist. Please run the Snowpark checkpoint collector first."
CHECKPOINT_JSON_OUTPUT_FILE_NOT_FOUND_ERROR: Final[
    str
] = "Checkpoint {} JSON file not found. Please run the Snowpark checkpoint collector first."
COLUMN_NAME_NOT_DEFINED_FORMAT_ERROR: Final[
    str
] = "Column name not defined in the schema: {}"
TYPE_NOT_DEFINED_FORMAT_ERROR: Final[str] = "Type not defined for column {}"

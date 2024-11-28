#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Skip type
SKIP_ALL = "skip_all"

# Supported types
BOOLEAN_TYPE = "boolean"
BINARY_TYPE = "binary"
BYTE_TYPE = "byte"
CHAR_TYPE = "char"
DATE_TYPE = "date"
DAYTIMEINTERVAL_TYPE = "daytimeinterval"
DECIMAL_TYPE = "decimal"
DOUBLE_TYPE = "double"
FLOAT_TYPE = "float"
INTEGER_TYPE = "integer"
LONG_TYPE = "long"
SHORT_TYPE = "short"
STRING_TYPE = "string"
TIMESTAMP_TYPE = "timestamp"
TIMESTAMP_NTZ_TYPE = "timestamp_ntz"
VARCHAR_TYPE = "varchar"

# Pandas data types
PANDAS_BOOLEAN_DTYPE = "bool"
PANDAS_DATETIME_DTYPE = "datetime64[ns]"
PANDAS_FLOAT_DTYPE = "float64"
PANDAS_INTEGER_DTYPE = "int64"
PANDAS_OBJECT_DTYPE = "object"
PANDAS_TIMEDELTA_DTYPE = "timedelta64[ns]"

# Shemas keys
COLUMNS_KEY = "columns"
COUNT_KEY = "rows_count"
DECIMAL_PRECISION_KEY = "decimal_precision"
FALSE_COUNT_KEY = "false_count"
FORMAT_KEY = "format"
NAME_KEY = "name"
MARGIN_ERROR_KEY = "margin_error"
MAX_KEY = "max"
MEAN_KEY = "mean"
MIN_KEY = "min"
NULL_COUNT_KEY = "rows_null_count"
ROWS_NOT_NULL_COUNT_KEY = "rows_not_null_count"
TRUE_COUNT_KEY = "true_count"
TYPE_KEY = "type"

DATAFRAME_CUSTOM_DATA_KEY = "custom_data"
DATAFRAME_PANDERA_SCHEMA_KEY = "pandera_schema"

# Format constants
BETWEEN_CHECK_ERROR_MESSAGE_FORMAT = "Value must be between {} and {}"
CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT = "snowpark-{}-schema-contract.json"

# Error messages
SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR = "Snowpark output schema validation error"

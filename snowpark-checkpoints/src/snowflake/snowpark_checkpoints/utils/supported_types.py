#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark_checkpoints.utils.constant import (
    BINARY_TYPE,
    BOOLEAN_TYPE,
    BYTE_TYPE,
    DATE_TYPE,
    DECIMAL_TYPE,
    DOUBLE_TYPE,
    FLOAT_TYPE,
    INTEGER_TYPE,
    LONG_TYPE,
    SHORT_TYPE,
    STRING_TYPE,
    TIMESTAMP_NTZ_TYPE,
    TIMESTAMP_TYPE,
)


NumericTypes = [
    BYTE_TYPE,
    SHORT_TYPE,
    INTEGER_TYPE,
    LONG_TYPE,
    FLOAT_TYPE,
    DOUBLE_TYPE,
    DECIMAL_TYPE,
]

StringTypes = [STRING_TYPE]

BinaryTypes = [BINARY_TYPE]

BooleanTypes = [BOOLEAN_TYPE]

DateTypes = [DATE_TYPE, TIMESTAMP_TYPE, TIMESTAMP_NTZ_TYPE]

SupportedTypes = [
    BYTE_TYPE,
    SHORT_TYPE,
    INTEGER_TYPE,
    LONG_TYPE,
    FLOAT_TYPE,
    DOUBLE_TYPE,
    DECIMAL_TYPE,
    STRING_TYPE,
    BINARY_TYPE,
    BOOLEAN_TYPE,
    DATE_TYPE,
    TIMESTAMP_TYPE,
    TIMESTAMP_NTZ_TYPE,
]

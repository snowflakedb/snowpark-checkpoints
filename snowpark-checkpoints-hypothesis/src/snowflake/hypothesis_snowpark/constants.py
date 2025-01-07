#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Final

from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampTimeZone,
    TimestampType,
)


PANDERA_SCHEMA_KEY: Final[str] = "pandera_schema"
PANDERA_IN_RANGE_CHECK: Final[str] = "in_range"
PANDERA_MIN_VALUE_KEY: Final[str] = "min_value"
PANDERA_MAX_VALUE_KEY: Final[str] = "max_value"
PANDERA_INCLUDE_MIN_KEY: Final[str] = "include_min"
PANDERA_INCLUDE_MAX_KEY: Final[str] = "include_max"

CUSTOM_DATA_KEY: Final[str] = "custom_data"
CUSTOM_DATA_FORMAT_KEY: Final[str] = "format"
CUSTOM_DATA_NAME_KEY: Final[str] = "name"
CUSTOM_DATA_COLUMNS_KEY: Final[str] = "columns"
CUSTOM_DATA_TYPE_KEY: Final[str] = "type"
CUSTOM_DATA_ROWS_COUNT: Final[str] = "rows_count"
CUSTOM_DATA_ROWS_NULL_COUNT_KEY: Final[str] = "rows_null_count"
CUSTOM_DATA_MIN_SIZE_KEY: Final[str] = "min_size"
CUSTOM_DATA_MAX_SIZE_KEY: Final[str] = "max_size"
CUSTOM_DATA_VALUE_TYPE_KEY: Final[str] = "value_type"

PYSPARK_ARRAY_TYPE: Final[str] = "array"
PYSPARK_BINARY_TYPE: Final[str] = "binary"
PYSPARK_BOOLEAN_TYPE: Final[str] = "boolean"
PYSPARK_BYTE_TYPE: Final[str] = "byte"
PYSPARK_DATE_TYPE: Final[str] = "date"
PYSPARK_DECIMAL_TYPE: Final[str] = "decimal"
PYSPARK_DOUBLE_TYPE: Final[str] = "double"
PYSPARK_FLOAT_TYPE: Final[str] = "float"
PYSPARK_INTEGER_TYPE: Final[str] = "integer"
PYSPARK_LONG_TYPE: Final[str] = "long"
PYSPARK_SHORT_TYPE: Final[str] = "short"
PYSPARK_STRING_TYPE: Final[str] = "string"
PYSPARK_TIMESTAMP_TYPE: Final[str] = "timestamp"
PYSPARK_TIMESTAMP_NTZ_TYPE: Final[str] = "timestamp_ntz"

PYSPARK_TO_SNOWPARK_SUPPORTED_TYPES: Final[dict[str, DataType]] = {
    PYSPARK_ARRAY_TYPE: ArrayType(),
    PYSPARK_BINARY_TYPE: BinaryType(),
    PYSPARK_BOOLEAN_TYPE: BooleanType(),
    PYSPARK_BYTE_TYPE: ByteType(),
    PYSPARK_DATE_TYPE: DateType(),
    PYSPARK_DECIMAL_TYPE: DecimalType(),
    PYSPARK_DOUBLE_TYPE: DoubleType(),
    PYSPARK_FLOAT_TYPE: FloatType(),
    PYSPARK_INTEGER_TYPE: IntegerType(),
    PYSPARK_LONG_TYPE: LongType(),
    PYSPARK_SHORT_TYPE: ShortType(),
    PYSPARK_STRING_TYPE: StringType(),
    PYSPARK_TIMESTAMP_TYPE: TimestampType(),
    PYSPARK_TIMESTAMP_NTZ_TYPE: TimestampType(TimestampTimeZone.NTZ),
}

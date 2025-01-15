#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
from unittest.mock import MagicMock

from pyspark.sql.types import StructType, StringType, StructField, IntegerType

from snowflake.snowpark_checkpoints_collector.collection_common import (
    INTEGER_COLUMN_TYPE,
    BOOLEAN_COLUMN_TYPE,
    DATE_COLUMN_TYPE,
    DAYTIMEINTERVAL_COLUMN_TYPE,
    DECIMAL_COLUMN_TYPE,
    STRING_COLUMN_TYPE,
    TIMESTAMP_COLUMN_TYPE,
    TIMESTAMP_NTZ_COLUMN_TYPE,
    ARRAY_COLUMN_TYPE,
    BINARY_COLUMN_TYPE,
    MAP_COLUMN_TYPE,
    NULL_COLUMN_TYPE,
    DOUBLE_COLUMN_TYPE,
)

from snowflake.snowpark_checkpoints_collector.column_collection.model import (
    BooleanColumnCollector,
    DateColumnCollector,
    DayTimeIntervalColumnCollector,
    DecimalColumnCollector,
    EmptyColumnCollector,
    NumericColumnCollector,
    StringColumnCollector,
    TimestampColumnCollector,
    TimestampNTZColumnCollector,
    ArrayColumnCollector,
    BinaryColumnCollector,
    MapColumnCollector,
    NullColumnCollector,
    StructColumnCollector,
)

ARRAY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "array", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "value_type": "integer", '
    '"allow_null": true, "null_value_proportion": 30.0, "max_size": 3, '
    '"min_size": 2, "mean_size": 2.5, "is_unique_size": false}'
)

BINARY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "binary", "nullable": true, "rows_count": 3, '
    '"rows_not_null_count": 2, "rows_null_count": 1, "max_size": 6, "min_size": 2, '
    '"mean_size": 4, "is_unique_size": false}'
)

BOOLEAN_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "boolean", "nullable": true, "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "true_count": 4, "false_count": 0}'
)

DATE_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "date", "nullable": true, "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "min": "YYYY-mm-dd", "max": "YYYY-mm-dd", "format": "%Y-%m-%d"}'
)

DAY_TIME_INTERVAL_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "daytimeinterval", "nullable": true, "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "min": "13 days 00:00:00", "max": "13 days 00:00:00"}'
)

DECIMAL_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "decimal", "nullable": true, "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "min": "0.000000000", "max": "0.000000000", "mean": "4", "decimal_precision": 10}'
)

EMPTY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "integer", "nullable": true, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0}'
)

EMPTY_ARRAY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "array", "nullable": true, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0, "value_type": "double", '
    '"allow_null": false}'
)

EMPTY_MAP_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "map", "nullable": true, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0, "key_type": "string", '
    '"value_type": "double", "allow_null": false}'
)

EMPTY_STRUCT_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "struct", "nullable": true, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0, "metadata": [{"name": '
    '"inner1", "type": "string", "nullable": true}, {"name": "inner2", '
    '"type": "integer", "nullable": true}]}'
)

MAP_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "map", "nullable": true, "rows_count": 4, '
    '"rows_not_null_count": 3, "rows_null_count": 1, "key_type": "string", "value_type": '
    '"integer", "allow_null": true, "null_value_proportion": 16.666666666666664, "max_size": '
    '3, "min_size": 1, "mean_size": 2, "is_unique_size": false}'
)

NULL_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "void", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 0, "rows_null_count": 5}'
)

NUMERIC_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "integer", "nullable": true, "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "min": 4, "max": 4, "mean": 4, "decimal_precision": 10, "margin_error": 4}'
)

STRING_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "string", "nullable": true, "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0}'
)

STRUCT_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "struct", "nullable": true, "rows_count": 4, '
    '"rows_not_null_count": 4, "rows_null_count": 0, "metadata": [{"name": "inner1", '
    '"type": "string", "nullable": true, "rows_count": 4, "rows_not_null_count": 3, '
    '"rows_null_count": 1}, {"name": "inner2", "type": "integer", "nullable": true, '
    '"rows_count": 4, "rows_not_null_count": 3, "rows_null_count": 1}]}'
)

TIMESTAMP_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "timestamp", "nullable": true, "rows_count": 4, '
    '"rows_not_null_count": 4, "rows_null_count": 0, "min": "YYYY-mm-dd HH:MM:SS", '
    '"max": "YYYY-mm-dd HH:MM:SS", "format": "%Y-%m-%dH:%M:%S"}'
)

TIMESTAMP_NTZ_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "timestamp_ntz", "nullable": true, "rows_count": '
    '4, "rows_not_null_count": 4, "rows_null_count": 0, "min": "YYYY-mm-dd '
    'HH:MM:SS z", "max": "YYYY-mm-dd HH:MM:SS z", "format": "%Y-%m-%dT%H:%M:%S%z"}'
)


def test_array_column_collection():
    clm_name = "clmTest"
    nullable = True
    allow_null = True
    json_mock = [INTEGER_COLUMN_TYPE, allow_null]

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    data_type_json_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = ARRAY_COLUMN_TYPE
    data_type_mock.jsonValue.return_value = data_type_json_mock
    data_type_json_mock.__getitem__.side_effect = json_mock

    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 5
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4
    series_mock.__iter__.return_value = [
        [1, 2, 3],
        [None, None, 6],
        [7, 8],
        [9, None],
        None,
    ]

    collector = ArrayColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == ARRAY_DATA_COLLECT_EXPECTED


def test_binary_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = BINARY_COLUMN_TYPE

    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 3
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 2
    series_mock.__iter__.return_value = [
        bytes([0x13, 0x00, 0x00, 0x00, 0x08, 0x00]),
        bytes([0x13, 0x13]),
        None,
    ]

    collector = BinaryColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == BINARY_DATA_COLLECT_EXPECTED


def test_boolean_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    series_without_na = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = BOOLEAN_COLUMN_TYPE

    series_mock.count.return_value = numpy_mock
    series_mock.dropna.return_value = series_without_na
    series_mock.__len__.return_value = 4

    series_without_na.where.return_value = series_without_na
    series_without_na.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    collector = BooleanColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == BOOLEAN_DATA_COLLECT_EXPECTED


def test_date_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    series_without_na = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = DATE_COLUMN_TYPE
    numpy_mock.item.return_value = 4

    series_mock.count.return_value = numpy_mock
    series_mock.dropna.return_value = series_without_na
    series_mock.__len__.return_value = 4

    series_without_na.where.return_value = series_without_na
    series_without_na.count.return_value = numpy_mock

    date_str = "YYYY-mm-dd"
    series_without_na.min.return_value = numpy_mock
    series_without_na.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = date_str

    collector = DateColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DATE_DATA_COLLECT_EXPECTED


def test_day_time_interval_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    series_without_na = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = DAYTIMEINTERVAL_COLUMN_TYPE

    series_mock.count.return_value = numpy_mock
    series_mock.dropna.return_value = series_without_na
    series_mock.__len__.return_value = 4

    series_without_na.where.return_value = series_without_na
    series_without_na.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    dayTimeInterval_str = "13 days 00:00:00"
    series_without_na.min.return_value = numpy_mock
    series_without_na.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = dayTimeInterval_str

    collector = DayTimeIntervalColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DAY_TIME_INTERVAL_DATA_COLLECT_EXPECTED


def test_decimal_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    series_without_na = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = DECIMAL_COLUMN_TYPE

    series_mock.count.return_value = numpy_mock
    series_mock.dropna.return_value = series_without_na
    series_mock.__len__.return_value = 4

    series_without_na.where.return_value = series_without_na
    series_without_na.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    decimal_str = "0.000000000"
    series_without_na.min.return_value = numpy_mock
    series_without_na.max.return_value = numpy_mock
    series_without_na.mean.return_value = numpy_mock
    numpy_mock.__str__.return_value = decimal_str

    collector = DecimalColumnCollector(clm_name, struct_field_mock, series_mock)
    DecimalColumnCollector._compute_decimal_precision = MagicMock(return_value=10)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DECIMAL_DATA_COLLECT_EXPECTED


def test_empty_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = INTEGER_COLUMN_TYPE

    series_mock.__len__.return_value = 0
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 0

    collector = EmptyColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_DATA_COLLECT_EXPECTED


def test_empty_array_column_collection():
    clm_name = "clmTest"
    nullable = True
    allow_null = False
    json_mock = [DOUBLE_COLUMN_TYPE, allow_null]

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    data_type_json_mock = MagicMock()
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = ARRAY_COLUMN_TYPE
    data_type_mock.jsonValue.return_value = data_type_json_mock
    data_type_json_mock.__getitem__.side_effect = json_mock

    series_mock.__len__.return_value = 0
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 0

    collector = EmptyColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_ARRAY_DATA_COLLECT_EXPECTED


def test_empty_map_column_collection():
    clm_name = "clmTest"
    nullable = True
    allow_null = False
    json_mock = [STRING_COLUMN_TYPE, DOUBLE_COLUMN_TYPE, allow_null]

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    data_type_json_mock = MagicMock()
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = MAP_COLUMN_TYPE
    data_type_mock.jsonValue.return_value = data_type_json_mock
    data_type_json_mock.__getitem__.side_effect = json_mock

    series_mock.__len__.return_value = 0
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 0

    collector = EmptyColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_MAP_DATA_COLLECT_EXPECTED


def test_empty_struct_column_collection():
    clm_name = "clmTest"
    nullable = True

    inner_schema = StructType(
        [
            StructField("inner1", StringType(), nullable),
            StructField("inner2", IntegerType(), nullable),
        ]
    )

    struct_field = StructField("c1", inner_schema, nullable)

    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 0
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 0

    collector = EmptyColumnCollector(clm_name, struct_field, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_STRUCT_DATA_COLLECT_EXPECTED


def test_map_column_collection():
    clm_name = "clmTest"
    nullable = True
    allow_null = True
    json_mock = [STRING_COLUMN_TYPE, INTEGER_COLUMN_TYPE, allow_null]

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    data_type_json_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = MAP_COLUMN_TYPE
    data_type_mock.jsonValue.return_value = data_type_json_mock
    data_type_json_mock.__getitem__.side_effect = json_mock

    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 3
    series_mock.__iter__.return_value = [
        {"A": 1, "B": 2, "C": None},
        {"D": 4, "E": 5},
        None,
        {"F": 6},
    ]

    collector = MapColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == MAP_DATA_COLLECT_EXPECTED


def test_null_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = NULL_COLUMN_TYPE

    series_mock.__len__.return_value = 5
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 0

    collector = NullColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == NULL_DATA_COLLECT_EXPECTED


def test_numeric_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    series_without_na = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = INTEGER_COLUMN_TYPE

    series_mock.count.return_value = numpy_mock
    series_mock.dropna.return_value = series_without_na
    series_mock.__len__.return_value = 4

    series_without_na.where.return_value = series_without_na
    series_without_na.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    series_without_na.min.return_value = numpy_mock
    series_without_na.max.return_value = numpy_mock
    series_without_na.mean.return_value = numpy_mock
    series_without_na.std.return_value = numpy_mock

    collector = NumericColumnCollector(clm_name, struct_field_mock, series_mock)
    NumericColumnCollector._compute_decimal_precision = MagicMock(return_value=10)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == NUMERIC_DATA_COLLECT_EXPECTED


def test_string_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = STRING_COLUMN_TYPE

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    collector = StringColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == STRING_DATA_COLLECT_EXPECTED


def test_struct_column_collection():
    clm_name = "clmTest"
    nullable = True

    inner_schema = StructType(
        [
            StructField("inner1", StringType(), nullable),
            StructField("inner2", IntegerType(), nullable),
        ]
    )

    struct_field = StructField("c1", inner_schema, nullable)

    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4
    series_mock.__iter__.return_value = [
        {"inner1": "A11", "inner2": 11},
        {"inner1": "A12", "inner2": 12},
        {"inner1": "A13", "inner2": 13},
        {"inner1": None, "inner2": None},
    ]

    collector = StructColumnCollector(clm_name, struct_field, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == STRUCT_DATA_COLLECT_EXPECTED


def test_timestamp_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    series_without_na = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = TIMESTAMP_COLUMN_TYPE

    series_mock.count.return_value = numpy_mock
    series_mock.dropna.return_value = series_without_na
    series_mock.__len__.return_value = 4

    series_without_na.where.return_value = series_without_na
    series_without_na.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    date_str = "YYYY-mm-dd HH:MM:SS"
    series_without_na.min.return_value = numpy_mock
    series_without_na.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = date_str

    collector = TimestampColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == TIMESTAMP_DATA_COLLECT_EXPECTED


def test_timestamp_ntz_column_collection():
    clm_name = "clmTest"
    nullable = True

    struct_field_mock = MagicMock()
    data_type_mock = MagicMock()
    series_mock = MagicMock()
    series_without_na = MagicMock()
    numpy_mock = MagicMock()

    struct_field_mock.nullable = nullable
    struct_field_mock.dataType = data_type_mock
    data_type_mock.typeName.return_value = TIMESTAMP_NTZ_COLUMN_TYPE

    series_mock.count.return_value = numpy_mock
    series_mock.dropna.return_value = series_without_na
    series_mock.__len__.return_value = 4

    series_without_na.where.return_value = series_without_na
    series_without_na.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    date_str = "YYYY-mm-dd HH:MM:SS z"
    series_without_na.min.return_value = numpy_mock
    series_without_na.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = date_str

    collector = TimestampNTZColumnCollector(clm_name, struct_field_mock, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == TIMESTAMP_NTZ_DATA_COLLECT_EXPECTED

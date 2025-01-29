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
import datetime
import decimal
import json
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    StructField,
    IntegerType,
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    MapType,
    NullType,
    DoubleType,
    TimestampType,
    TimestampNTZType,
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
    '"rows_not_null_count": 4, "rows_null_count": 1, "value_type": "string", "allow_null": '
    'true, "null_value_proportion": 30.0, "max_size": 4, "min_size": 0, "mean_size": 2, '
    '"is_unique_size": false}'
)

BINARY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "binary", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "max_size": 6, "min_size": 0, '
    '"mean_size": 2.6, "is_unique_size": false}'
)

BOOLEAN_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "boolean", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "true_count": 1, "false_count": 3}'
)

DATE_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "date", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "min": "1990-09-09", '
    '"max": "2020-05-20", "format": "%Y-%m-%d"}'
)

DAY_TIME_INTERVAL_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "daytimeinterval", "nullable": true, '
    '"rows_count": 5, "rows_not_null_count": 4, "rows_null_count": 1, '
    '"min": "13 days, 0:00:00", "max": "20 days, 0:00:00"}'
)

DECIMAL_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "decimal", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "min": "1.000000000", '
    '"max": "4.567800190", "mean": "2.7037867447500", "decimal_precision": 9}'
)

EMPTY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "string", "nullable": false, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0}'
)

EMPTY_ARRAY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "array", "nullable": false, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0, "value_type": "string", '
    '"allow_null": false}'
)

EMPTY_MAP_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "map", "nullable": false, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0, "key_type": "string", '
    '"value_type": "string", "allow_null": false}'
)

EMPTY_STRUCT_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "struct", "nullable": true, "rows_count": 0, '
    '"rows_not_null_count": 0, "rows_null_count": 0, "metadata": [{"name": '
    '"inner1", "type": "string", "nullable": true}, {"name": "inner2", '
    '"type": "integer", "nullable": true}]}'
)

MAP_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "map", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "key_type": "string", "value_type": '
    '"integer", "allow_null": true, "null_value_proportion": 33.33333333333333, "max_size": '
    '3, "min_size": 0, "mean_size": 1.8, "is_unique_size": false}'
)

NULL_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "void", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 0, "rows_null_count": 5}'
)

NUMERIC_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "double", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "min": 1.000001, "max": 13.14, '
    '"mean": 7.04085025, "decimal_precision": 6, "margin_error": 5.190521508426064}'
)

STRING_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "string", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "min_length": 1, "max_length": 9}'
)

STRUCT_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "struct", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "metadata": [{"name": "inner1", '
    '"type": "string", "nullable": true, "rows_count": 5, "rows_not_null_count": 3, '
    '"rows_null_count": 2}, {"name": "inner2", "type": "integer", "nullable": true, '
    '"rows_count": 5, "rows_not_null_count": 3, "rows_null_count": 2}]}'
)

TIMESTAMP_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "timestamp", "nullable": true, "rows_count": 5, '
    '"rows_not_null_count": 4, "rows_null_count": 1, "min": "1998-05-05 05:05:05", '
    '"max": "2025-12-20 00:00:00", "format": "%Y-%m-%dT%H:%M:%S%z"}'
)

TIMESTAMP_NTZ_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "timestamp_ntz", "nullable": true, "rows_count": '
    '5, "rows_not_null_count": 4, "rows_null_count": 1, "min": "1998-05-05 '
    '05:05:05", "max": "2025-12-20 00:00:00", "format": "%Y-%m-%dH:%M:%S"}'
)


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_array_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, ArrayType(StringType(), True), True)
    data = [
        [["A1", "A2"]],
        [["B1", "B2"]],
        [["C1", "C2", "C3", None]],
        [[None, None]],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data=data, schema=schema)

    collector = ArrayColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == ARRAY_DATA_COLLECT_EXPECTED


def test_binary_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, BinaryType(), True)
    data = [
        [bytes([0x13, 0x01])],
        [bytes([0x13, 0x02])],
        [bytes([0x15, 0x06, 0x00, 0x00, 0x08, 0x00])],
        [bytes([0x00, 0x08, 0x00])],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data=data, schema=schema)

    collector = BinaryColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == BINARY_DATA_COLLECT_EXPECTED


def test_boolean_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, BooleanType(), True)
    data = [[True], [False], [False], [False], [None]]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = BooleanColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == BOOLEAN_DATA_COLLECT_EXPECTED


def test_date_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, DateType(), True)
    data = [
        [datetime.date(2000, 1, 1)],
        [datetime.date(1990, 9, 9)],
        [datetime.date(2010, 12, 13)],
        [datetime.date(2020, 5, 20)],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = DateColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DATE_DATA_COLLECT_EXPECTED


def test_day_time_interval_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, DayTimeIntervalType(), True)
    data = [
        [datetime.timedelta(13)],
        [datetime.timedelta(14)],
        [datetime.timedelta(15)],
        [datetime.timedelta(20)],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = DayTimeIntervalColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DAY_TIME_INTERVAL_DATA_COLLECT_EXPECTED


def test_decimal_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, DecimalType(10, 9), True)
    data = [
        [decimal.Decimal("1")],
        [decimal.Decimal("2.123456789")],
        [decimal.Decimal("3.123890")],
        [decimal.Decimal("4.56780019")],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = DecimalColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DECIMAL_DATA_COLLECT_EXPECTED


def test_empty_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, StringType(), False)
    data = []
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = EmptyColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_DATA_COLLECT_EXPECTED


def test_empty_array_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, ArrayType(StringType(), False), False)
    data = []
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = EmptyColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_ARRAY_DATA_COLLECT_EXPECTED


def test_empty_map_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(
        clm_name, MapType(StringType(), StringType(), False), False
    )
    data = []
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = EmptyColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_MAP_DATA_COLLECT_EXPECTED


def test_empty_struct_column_collection(spark_session):
    clm_name = "clmTest"
    nullable = True

    inner_schema = StructType(
        [
            StructField("inner1", StringType(), nullable),
            StructField("inner2", IntegerType(), nullable),
        ]
    )

    struct_field = StructField("c1", inner_schema, nullable)

    data = []
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = EmptyColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_STRUCT_DATA_COLLECT_EXPECTED


def test_map_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(
        clm_name, MapType(StringType(), IntegerType(), True), True
    )
    data = [
        [{"a": 1}],
        [{"b": 2, "c": 3, "d": 4}],
        [{"e": 5, "f": None}],
        [{"g": 7, "h": None, "i": None}],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = MapColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == MAP_DATA_COLLECT_EXPECTED


def test_null_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, NullType(), True)
    data = [[None], [None], [None], [None], [None]]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = NullColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == NULL_DATA_COLLECT_EXPECTED


def test_numeric_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, DoubleType(), True)
    data = [[5.1234], [13.14], [1.000001], [8.90], [None]]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = NumericColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == NUMERIC_DATA_COLLECT_EXPECTED


def test_string_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, StringType(), True)
    data = [["Batman"], ["September"], ["Robin"], ["A"], [None]]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = StringColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == STRING_DATA_COLLECT_EXPECTED


def test_struct_column_collection(spark_session):
    clm_name = "clmTest"

    data = [
        [{"inner1": "a", "inner2": 1}],
        [{"inner1": "b", "inner2": 2}],
        [{"inner1": "c", "inner2": 3}],
        [{"inner1": None, "inner2": None}],
        [None],
    ]

    inner_schema = StructType(
        [
            StructField("inner1", StringType(), True),
            StructField("inner2", IntegerType(), True),
        ]
    )
    struct_field = StructField(clm_name, inner_schema, True)
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = StructColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == STRUCT_DATA_COLLECT_EXPECTED


def test_timestamp_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, TimestampType(), True)
    data = [
        [datetime.datetime(2000, 10, 10, 10, 10, 10)],
        [datetime.datetime(1998, 5, 5, 5, 5, 5)],
        [datetime.datetime(2012, 3, 7, 2, 0, 10)],
        [datetime.datetime(2025, 12, 20, 0, 0, 0)],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = TimestampColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == TIMESTAMP_DATA_COLLECT_EXPECTED


def test_timestamp_ntz_column_collection(spark_session):
    clm_name = "clmTest"
    struct_field = StructField(clm_name, TimestampNTZType(), True)
    data = [
        [datetime.datetime(2000, 10, 10, 10, 10, 10)],
        [datetime.datetime(1998, 5, 5, 5, 5, 5)],
        [datetime.datetime(2012, 3, 7, 2, 0, 10)],
        [datetime.datetime(2025, 12, 20, 0, 0, 0)],
        [None],
    ]
    schema = StructType([struct_field])
    column_values = spark_session.createDataFrame(data, schema)

    collector = TimestampNTZColumnCollector(clm_name, struct_field, column_values)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == TIMESTAMP_NTZ_DATA_COLLECT_EXPECTED

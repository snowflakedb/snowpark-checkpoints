#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
from unittest.mock import MagicMock
from snowflake.snowpark_checkpoints_collector.collection_common import (
    BOOLEAN_COLUMN_TYPE,
    DATE_COLUMN_TYPE,
    DAYTIMEINTERVAL_COLUMN_TYPE,
    INTEGER_COLUMN_TYPE,
    STRING_COLUMN_TYPE,
    TIMESTAMP_COLUMN_TYPE,
    TIMESTAMP_NTZ_COLUMN_TYPE,
)

from snowflake.snowpark_checkpoints_collector.column_collection.model import (
    BooleanColumnCollector,
    DateColumnCollector,
    DayTimeIntervalColumnCollector,
    EmptyColumnCollector,
    NumericColumnCollector,
    StringColumnCollector,
    TimestampColumnCollector,
    TimestampNTZColumnCollector,
)

BOOLEAN_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "boolean", "rows_count": 4, "rows_not_null_count": 4, "rows_null_count": 0, '
    '"true_count": 4, "false_count": 0}'
)

DATE_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "date", "rows_count": 4, "rows_not_null_count": 4, "rows_null_count": 0, '
    '"min": "YYYY-mm-dd", "max": "YYYY-mm-dd", "format": "%Y-%m-%d"}'
)

DAY_TIME_INTERVAL_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "daytimeinterval", "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "min": "13 days 00:00:00", "max": "13 days '
    '00:00:00"}'
)

EMPTY_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "integer", "rows_count": 0, "rows_not_null_count": 0, '
    '"rows_null_count": 0}'
)

NUMERIC_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "integer", "rows_count": 4, "rows_not_null_count": 4, "rows_null_count": 0, '
    '"min": 4, "max": 4, "mean": 4, "decimal_precision": 0, "margin_error": 4}'
)

STRING_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "string", "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0}'
)

TIMESTAMP_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "timestamp", "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "min": "YYYY-mm-dd HH:MM:SS", "max": "YYYY-mm-dd HH:MM:SS",'
    ' "format": "%Y-%m-%dH:%M:%S"}'
)

TIMESTAMP_NTZ_DATA_COLLECT_EXPECTED = (
    '{"name": "clmTest", "type": "timestamp_ntz", "rows_count": 4, "rows_not_null_count": 4, '
    '"rows_null_count": 0, "min": "YYYY-mm-dd HH:MM:SS z", "max": "YYYY-mm-dd '
    'HH:MM:SS z", "format": "%Y-%m-%dT%H:%M:%S%z"}'
)


def test_boolean_column_collection():
    clm_name = "clmTest"

    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.where.return_value = series_mock
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    collector = BooleanColumnCollector(clm_name, BOOLEAN_COLUMN_TYPE, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == BOOLEAN_DATA_COLLECT_EXPECTED


def test_date_column_collection():
    clm_name = "clmTest"
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    date_str = "YYYY-mm-dd"
    series_mock.min.return_value = numpy_mock
    series_mock.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = date_str

    collector = DateColumnCollector(clm_name, DATE_COLUMN_TYPE, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DATE_DATA_COLLECT_EXPECTED


def test_day_time_interval_column_collection():
    clm_name = "clmTest"
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    dayTimeInterval_str = "13 days 00:00:00"
    series_mock.min.return_value = numpy_mock
    series_mock.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = dayTimeInterval_str

    collector = DayTimeIntervalColumnCollector(
        clm_name, DAYTIMEINTERVAL_COLUMN_TYPE, series_mock
    )
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == DAY_TIME_INTERVAL_DATA_COLLECT_EXPECTED


def test_empty_column_collection():
    clm_name = "clmTest"
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 0
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 0

    collector = EmptyColumnCollector(clm_name, INTEGER_COLUMN_TYPE, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == EMPTY_DATA_COLLECT_EXPECTED


def test_numeric_column_collection():
    clm_name = "clmTest"
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    series_mock.min.return_value = numpy_mock
    series_mock.max.return_value = numpy_mock
    series_mock.mean.return_value = numpy_mock
    series_mock.std.return_value = numpy_mock

    collector = NumericColumnCollector(clm_name, INTEGER_COLUMN_TYPE, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == NUMERIC_DATA_COLLECT_EXPECTED


def test_string_column_collection():
    clm_name = "clmTest"
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    collector = StringColumnCollector(clm_name, STRING_COLUMN_TYPE, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == STRING_DATA_COLLECT_EXPECTED


def test_timestamp_column_collection():
    clm_name = "clmTest"
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    date_str = "YYYY-mm-dd HH:MM:SS"
    series_mock.min.return_value = numpy_mock
    series_mock.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = date_str

    collector = TimestampColumnCollector(clm_name, TIMESTAMP_COLUMN_TYPE, series_mock)
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == TIMESTAMP_DATA_COLLECT_EXPECTED


def test_timestamp_ntz_column_collection():
    clm_name = "clmTest"
    series_mock = MagicMock()
    numpy_mock = MagicMock()

    series_mock.__len__.return_value = 4
    series_mock.count.return_value = numpy_mock
    numpy_mock.item.return_value = 4

    date_str = "YYYY-mm-dd HH:MM:SS z"
    series_mock.min.return_value = numpy_mock
    series_mock.max.return_value = numpy_mock
    numpy_mock.__str__.return_value = date_str

    collector = TimestampNTZColumnCollector(
        clm_name, TIMESTAMP_NTZ_COLUMN_TYPE, series_mock
    )
    data_collected = collector.get_data()

    data_collected_json = json.dumps(data_collected)
    assert data_collected_json == TIMESTAMP_NTZ_DATA_COLLECT_EXPECTED

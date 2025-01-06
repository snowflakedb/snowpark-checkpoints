#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Iterable
from datetime import date, datetime
from typing import Union

import hypothesis.strategies as st
import pytest

from hypothesis import HealthCheck, given, settings

from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, count, when
from snowflake.snowpark.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)


def test_dataframe_strategy_none_schema(local_session: Session):
    with pytest.raises(ValueError, match="JSON schema cannot be None."):
        dataframe_strategy(json_schema=None, session=local_session)


def test_dataframe_strategy_none_session():
    with pytest.raises(ValueError, match="Session cannot be None."):
        dataframe_strategy(json_schema="schema.json", session=None)


def test_dataframe_strategy_invalid_json_file(local_session: Session):
    with pytest.raises(
        ValueError,
        match="Invalid JSON schema. The JSON schema must contain 'pandera_schema' and 'custom_data' keys.",
    ):
        dataframe_strategy(
            json_schema="test/resources/invalid_json.json", session=local_session
        )


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_non_nullable_columns(
    data: st.DataObject, local_session: Session
):
    strategy = dataframe_strategy(
        json_schema="test/resources/non_nullable_columns.json",
        session=local_session,
    )

    df = data.draw(strategy)

    null_count_df = df.select(
        count(when(col("A").is_null(), 1)).alias("null_count"),
    ).collect()
    assert len(null_count_df) == 1

    actual_null_count = null_count_df[0]["NULL_COUNT"]
    assert (
        actual_null_count == 0
    ), f"Expected 0 null values, but got {actual_null_count}"


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_nullable_column(
    data: st.DataObject, local_session: Session
):
    number_of_rows = 8

    strategy = dataframe_strategy(
        json_schema="test/resources/nullable_columns.json",
        session=local_session,
        size=number_of_rows,
    )

    df = data.draw(strategy)

    null_counts = df.select(
        count(when(col("A").is_null(), 1)).alias("null_count"),
    ).collect()
    assert len(null_counts) == 1

    actual_null_count = null_counts[0]["NULL_COUNT"]
    expected_min_null_count = number_of_rows // 2
    expected_max_null_count = number_of_rows
    assert (
        expected_min_null_count <= actual_null_count <= expected_max_null_count
    ), f"Expected {expected_min_null_count} <= null values <= {expected_max_null_count}, but got {actual_null_count}."


@given(data=st.data())
@settings(deadline=None, max_examples=10, suppress_health_check=list(HealthCheck))
def test_dataframe_strategy_generated_schema(
    data: st.DataObject, local_session: Session
):
    strategy = dataframe_strategy(
        json_schema="test/resources/supported_columns.json",
        session=local_session,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    expected_schema = StructType(
        [
            StructField("boolean_column", BooleanType(), False),
            StructField("byte_column", LongType(), False),
            StructField("date_column", DateType(), False),
            StructField("double_column", DoubleType(), False),
            StructField("float_column", DoubleType(), False),
            StructField("integer_column", LongType(), False),
            StructField("long_column", LongType(), False),
            StructField("short_column", LongType(), False),
            StructField("string_column", StringType(), False),
            StructField(
                "timestamp_column", TimestampType(TimestampTimeZone.NTZ), False
            ),
            StructField(
                "timestampNTZ_column", TimestampType(TimestampTimeZone.NTZ), False
            ),
        ]
    )

    assert df.schema == expected_schema


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_generated_values(
    data: st.DataObject, local_session: Session
):
    strategy = dataframe_strategy(
        json_schema="test/resources/supported_columns.json",
        session=local_session,
    )

    df = data.draw(strategy)

    def range_check(
        column_name: str, min_value: Union[float, date], max_value: Union[float, date]
    ):
        return (col(column_name) >= min_value) & (col(column_name) <= max_value)

    def in_check(column_name: str, values: Iterable):
        return col(column_name).isin(values)

    column_checks = {
        "BOOLEAN_COLUMN": in_check("BOOLEAN_COLUMN", [True, False]),
        "BYTE_COLUMN": range_check("BYTE_COLUMN", -128, 127),
        "DATE_COLUMN": range_check("DATE_COLUMN", date(2020, 1, 16), date(2024, 11, 1)),
        "DOUBLE_COLUMN": range_check(
            "DOUBLE_COLUMN", -2.6692257258090617, 2.5378806273606926
        ),
        "FLOAT_COLUMN": range_check(
            "FLOAT_COLUMN", -2.7640867233276367, 3.1381418704986572
        ),
        "INTEGER_COLUMN": range_check("INTEGER_COLUMN", -148, 138),
        "LONG_COLUMN": range_check("LONG_COLUMN", -29777341365, 29631563833),
        "SHORT_COLUMN": range_check("SHORT_COLUMN", -95, 100),
        "STRING_COLUMN": col("STRING_COLUMN").cast("String") == col("STRING_COLUMN"),
        "TIMESTAMP_COLUMN": range_check(
            "TIMESTAMP_COLUMN",
            datetime(2020, 3, 19, 13, 55, 59, 121579),
            datetime(2024, 11, 25, 7, 50, 16, 682043),
        ),
        "TIMESTAMPNTZ_COLUMN": range_check(
            "TIMESTAMPNTZ_COLUMN",
            datetime(2020, 1, 1, 6, 29, 59, 768559),
            datetime(2024, 11, 7, 14, 24, 40, 338141),
        ),
    }

    for column, condition in column_checks.items():
        invalid_rows = df.filter(~condition)
        invalid_count = invalid_rows.count()
        assert invalid_count == 0, (
            f"Column '{column}' contains invalid values."
            f"Actual values: {invalid_rows.collect()}"
        )

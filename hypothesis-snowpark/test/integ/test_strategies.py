#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from datetime import datetime

import hypothesis.strategies as st
import pytest

from hypothesis import given, settings

from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, count, when
from snowflake.snowpark.types import (
    BooleanType,
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
            json_schema="resources/invalid_json.json", session=local_session
        )


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_non_nullable_columns(
    data: st.DataObject, local_session: Session
):
    strategy = dataframe_strategy(
        json_schema="resources/non_nullable_columns.json",
        session=local_session,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    null_counts = df.select(
        count(when(col("A").is_null(), 1)).alias("null_count"),
        count(when(col("A").is_not_null(), 1)).alias("non_null_count"),
    ).collect()
    assert len(null_counts) == 1

    expected_null_count = 0
    actual_null_count = null_counts[0]["NULL_COUNT"]
    assert expected_null_count == actual_null_count, (
        f"Expected {expected_null_count} null values, but got {actual_null_count}. "
        f"Actual rows: {null_counts}"
    )

    expected_non_null_count = df.count()
    actual_non_null_count = null_counts[0]["NON_NULL_COUNT"]
    assert expected_non_null_count == actual_non_null_count, (
        f"Expected {expected_non_null_count} non-null values, but got {actual_non_null_count}. "
        f"Actual rows: {null_counts}"
    )


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_nullable_column(
    data: st.DataObject, local_session: Session
):
    number_of_rows = 8
    strategy = dataframe_strategy(
        json_schema="resources/nullable_columns.json",
        session=local_session,
        size=number_of_rows,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    null_counts = df.select(
        count(when(col("A").is_null(), 1)).alias("null_count"),
        count(when(col("A").is_not_null(), 1)).alias("non_null_count"),
    ).collect()
    assert len(null_counts) == 1

    actual_null_count = null_counts[0]["NULL_COUNT"]
    expected_min_null_count = number_of_rows / 2
    expected_max_null_count = number_of_rows
    assert expected_min_null_count <= actual_null_count <= expected_max_null_count, (
        f"Expected {expected_min_null_count} <= null values <= {expected_max_null_count}, but got {actual_null_count}. "
        f"Actual rows: {null_counts}"
    )

    actual_non_null_count = null_counts[0]["NON_NULL_COUNT"]
    expected_min_non_null_count = 0
    expected_max_non_null_count = number_of_rows / 2
    assert (
        expected_min_non_null_count
        <= actual_non_null_count
        <= expected_max_non_null_count
    ), (
        f"Expected {expected_min_non_null_count} <= non-null values <= {expected_max_non_null_count}, \n"
        f"but got {actual_non_null_count}. "
        f"Actual rows: {null_counts}"
    )


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_generated_schema(
    data: st.DataObject, local_session: Session
):
    strategy = dataframe_strategy(
        json_schema="resources/supported_columns.json",
        session=local_session,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    expected_schema = StructType(
        [
            StructField("boolean_column", BooleanType(), False),
            StructField("byte_column", LongType(), False),
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
    expected_constraints = {
        "BOOLEAN_COLUMN": {"type": "bool", "allowed_values": [True, False]},
        "BYTE_COLUMN": {"type": "numeric", "range": (-128, 127)},
        "DOUBLE_COLUMN": {
            "type": "numeric",
            "range": (-2.6692257258090617, 2.5378806273606926),
        },
        "FLOAT_COLUMN": {
            "type": "numeric",
            "range": (-2.7640867233276367, 3.1381418704986572),
        },
        "INTEGER_COLUMN": {"type": "numeric", "range": (-148, 138)},
        "LONG_COLUMN": {"type": "numeric", "range": (-29777341365, 29631563833)},
        "SHORT_COLUMN": {"type": "numeric", "range": (-95, 100)},
        "STRING_COLUMN": {"type": "string"},
        "TIMESTAMP_COLUMN": {
            "type": "time",
            "range": (
                datetime(2020, 3, 19, 13, 55, 59, 121579),
                datetime(2024, 11, 25, 7, 50, 16, 682043),
            ),
        },
        "TIMESTAMPNTZ_COLUMN": {
            "type": "time",
            "range": (
                datetime(2020, 1, 1, 6, 29, 59, 768559),
                datetime(2024, 11, 7, 14, 24, 40, 338141),
            ),
        },
    }

    strategy = dataframe_strategy(
        json_schema="resources/supported_columns.json",
        session=local_session,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    for column_name, constraints in expected_constraints.items():
        column_rows = df.select(col(column_name)).collect()
        column_values = [row[column_name] for row in column_rows]

        if constraints["type"] == "bool":
            allowed_values = constraints["allowed_values"]
            assert all(value in allowed_values for value in column_values), (
                f"Values in column '{column_name}' are invalid. "
                f"Expected values: {allowed_values}. "
                f"Found values: {column_values}"
            )
        elif constraints["type"] in ("numeric", "time"):
            min_val, max_val = constraints["range"]
            assert all(min_val <= value <= max_val for value in column_values), (
                f"Values in column '{column_name}' are out of range. "
                f"Expected range: ({min_val}, {max_val}). "
                f"Found values: {column_values}"
            )
        elif constraints["type"] == "string":
            assert all(isinstance(value, str) for value in column_values), (
                f"Values in column '{column_name}' are not strings. "
                f"Found values: {column_values}"
            )

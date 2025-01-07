#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import ast

import os
import json
import shutil
from deepdiff import DeepDiff

from collections.abc import Iterable
from datetime import date, datetime

import hypothesis.strategies as st
import pandera as pa

from hypothesis import HealthCheck, given, settings

from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.hypothesis_snowpark.telemetry.telemetry import get_telemetry_manager
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.functions import col, count, when

from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
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

SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME = "snowpark-checkpoints-output"
TEST_TELEMETRY_DATAFRAME_STRATEGIES_EXPECTED_DIRECTORY_NAME = (
    "test_telemetry_strategies_expected"
)


NTZ = TimestampTimeZone.NTZ


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
    validate_telemetry_file_output(
        "test_dataframe_strategy_nullable_column_telemetry.json"
    )


@given(data=st.data())
@settings(deadline=None, max_examples=10, suppress_health_check=list(HealthCheck))
def test_dataframe_strategy_generated_schema(data: st.DataObject, session: Session):
    strategy = dataframe_strategy(
        json_schema="test/resources/supported_columns.json",
        session=session,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    expected_schema = StructType(
        [
            StructField("array_column", ArrayType(), False),
            StructField("binary_column", BinaryType(), False),
            StructField("boolean_column", BooleanType(), False),
            StructField("byte_column", LongType(), False),
            StructField("date_column", DateType(), False),
            StructField("double_column", DoubleType(), False),
            StructField("float_column", DoubleType(), False),
            StructField("integer_column", LongType(), False),
            StructField("long_column", LongType(), False),
            StructField("short_column", LongType(), False),
            StructField("string_column", StringType(), False),
            StructField("timestamp_column", TimestampType(NTZ), False),
            StructField("timestampNTZ_column", TimestampType(NTZ), False),
        ]
    )

    assert df.schema == expected_schema
    validate_telemetry_file_output(
        "test_dataframe_strategy_generated_schema_telemetry.json"
    )


@given(data=st.data())
@settings(deadline=None, max_examples=10, suppress_health_check=list(HealthCheck))
def test_dataframe_strategy_generated_values(data: st.DataObject, session: Session):
    strategy = dataframe_strategy(
        json_schema="test/resources/supported_columns.json",
        session=session,
    )

    df = data.draw(strategy)

    expected_schema = pa.DataFrameSchema(
        {
            "ARRAY_COLUMN": pa.Column(
                checks=pa.Check(
                    lambda series: series.apply(
                        lambda row: isinstance(row, list)
                        and all(isinstance(element, int) for element in row)
                    ),
                    error="Column must contain lists of integers",
                ),
            ),
            "BINARY_COLUMN": pa.Column(
                checks=pa.Check(
                    lambda series: series.apply(
                        lambda row: isinstance(row, bytes) and 2 <= len(row) <= 6
                    )
                )
            ),
            "BOOLEAN_COLUMN": pa.Column(
                checks=pa.Check.isin(
                    [True, False],
                ),
            ),
            "BYTE_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    -128,
                    127,
                ),
            ),
            "DATE_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    date(2020, 1, 16),
                    date(2024, 11, 1),
                )
            ),
            "DOUBLE_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    -2.6692257258090617,
                    2.5378806273606926,
                )
            ),
            "FLOAT_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    -2.7640867233276367,
                    3.1381418704986572,
                )
            ),
            "INTEGER_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    -148,
                    138,
                )
            ),
            "LONG_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    -29777341365,
                    29631563833,
                )
            ),
            "SHORT_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    -95,
                    100,
                )
            ),
            "STRING_COLUMN": pa.Column(
                checks=pa.Check(
                    lambda series: series.apply(lambda row: isinstance(row, str))
                ),
            ),
            "TIMESTAMP_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    datetime(2020, 3, 19, 13, 55, 59, 121579),
                    datetime(2024, 11, 25, 7, 50, 16, 682043),
                )
            ),
            "TIMESTAMPNTZ_COLUMN": pa.Column(
                checks=pa.Check.in_range(
                    datetime(2020, 1, 1, 6, 29, 59, 768559),
                    datetime(2024, 11, 7, 14, 24, 40, 338141),
                )
            ),
        }
    )

    pandas_df = df.toPandas()
    pandas_df["ARRAY_COLUMN"] = pandas_df["ARRAY_COLUMN"].apply(ast.literal_eval)

    try:
        expected_schema.validate(pandas_df)
    except pa.errors.SchemaError as e:
        raise AssertionError(f"Schema validation failed: {e.args[0]}.\n") from e
    validate_telemetry_file_output(
        "test_dataframe_strategy_generated_values_telemetry.json"
    )


def get_expected(file_name) -> str:
    current_directory_path = os.path.dirname(__file__)
    expected_file_path = os.path.join(
        current_directory_path,
        TEST_TELEMETRY_DATAFRAME_STRATEGIES_EXPECTED_DIRECTORY_NAME,
        file_name,
    )

    with open(expected_file_path) as f:
        return f.read().strip()


def remove_output_directory() -> None:
    current_directory_path = os.getcwd()
    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    if os.path.exists(output_directory_path):
        shutil.rmtree(output_directory_path)


def get_output_telemetry() -> str:
    current_directory_path = os.getcwd()
    telemetry_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME, "telemetry"
    )
    for file in os.listdir(telemetry_directory_path):
        if file.endswith(".json"):
            output_file_path = os.path.join(telemetry_directory_path, file)
            with open(output_file_path) as f:
                return f.read().strip()
    return "{}"


def validate_telemetry_file_output(telemetry_file_name) -> None:
    telemetry_expected = get_expected(telemetry_file_name)
    telemetry_output = get_output_telemetry()

    telemetry_expected_obj = json.loads(telemetry_expected)
    telemetry_output_obj = json.loads(telemetry_output)
    exclude_telemetry_paths = [
        "root['timestamp']",
        "root['message']['metadata']['device_id']",
        "root['message']['metadata']",
        "root['message']['driver_version']",
    ]

    diff_telemetry = DeepDiff(
        telemetry_expected_obj,
        telemetry_output_obj,
        ignore_order=True,
        exclude_paths=exclude_telemetry_paths,
    )
    remove_output_directory()
    get_telemetry_manager().sc_hypothesis_input_events = []

    assert diff_telemetry == {}

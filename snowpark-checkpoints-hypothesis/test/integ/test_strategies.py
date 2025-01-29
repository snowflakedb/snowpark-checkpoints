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

import ast
import json
import os
import shutil

from datetime import date, datetime
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import hypothesis.strategies as st
import pandas as pd
import pandera as pa
import pytest

from deepdiff import DeepDiff
from hypothesis import HealthCheck, given, settings

from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.hypothesis_snowpark.telemetry.telemetry import get_telemetry_manager
from snowflake.snowpark import DataFrame, Row, Session
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
TZ = TimestampTimeZone.TZ


@pytest.fixture(scope="session", autouse=True)
def telemetry_testing_mode():
    telemetry_manager = get_telemetry_manager()
    telemetry_manager.sc_is_testing = True
    telemetry_manager.sc_is_enabled = True


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_non_nullable_columns(
    data: st.DataObject, local_session: Session
):
    strategy = dataframe_strategy(
        schema="test/resources/non_nullable_columns.json",
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
        schema="test/resources/nullable_columns.json",
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
def test_dataframe_strategy_from_json_schema_generated_schema(
    data: st.DataObject, session: Session
):
    strategy = dataframe_strategy(
        schema="test/resources/supported_columns.json",
        session=session,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    expected_schema = StructType(
        [
            StructField("array_column", ArrayType(), True),
            StructField("binary_column", BinaryType(), False),
            StructField("boolean_column", BooleanType(), True),
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
def test_dataframe_strategy_from_json_schema_generated_values(
    data: st.DataObject, session: Session
):
    strategy = dataframe_strategy(
        schema="test/resources/supported_columns.json",
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


@given(data=st.data())
def test_dataframe_strategy_from_object_schema_missing_dtype(data: st.DataObject):
    schema = pa.DataFrameSchema(
        {
            "integer_column": pa.Column(checks=pa.Check.in_range(0, 10)),
        }
    )

    strategy = dataframe_strategy(
        schema=schema,
        session=Mock(spec=Session),
    )

    with pytest.raises(
        pa.errors.SchemaDefinitionError,
        match=(
            "'column' schema with name 'integer_column' has no specified dtype. "
            "You need to specify one in order to synthesize data from a strategy."
        ),
    ):
        data.draw(strategy)
    validate_telemetry_file_output(
        "test_dataframe_strategy_from_object_schema_missing_dtype_telemetry.json"
    )


@given(data=st.data())
@settings(deadline=None, max_examples=5, suppress_health_check=list(HealthCheck))
def test_dataframe_strategy_from_object_schema_generated_schema(
    data: st.DataObject, session: Session
):
    schema = pa.DataFrameSchema(
        {
            "byte_column": pa.Column(pa.Int8, nullable=True),
            "short_column": pa.Column(pa.Int16, nullable=True),
            "integer_column": pa.Column(pa.Int32, nullable=True),
            "long_column": pa.Column(pa.Int64, nullable=True),
            "float_column": pa.Column(pa.Float32, nullable=True),
            "double_column": pa.Column(pa.Float64, nullable=True),
            "string_column": pa.Column(pa.String, nullable=True),
            "boolean_column": pa.Column(pa.Bool, nullable=True),
            "timestamp_column": pa.Column(
                pd.DatetimeTZDtype(tz=ZoneInfo("UTC")), nullable=True
            ),
            "timestampNTZ_column": pa.Column(pa.Timestamp, nullable=True),
            "date_column": pa.Column(pa.Date, nullable=True),
        }
    )

    strategy = dataframe_strategy(
        schema=schema,
        session=session,
    )

    df = data.draw(strategy)
    assert isinstance(df, DataFrame)

    expected_schema = StructType(
        [
            StructField("byte_column", LongType(), True),
            StructField("short_column", LongType(), True),
            StructField("integer_column", LongType(), True),
            StructField("long_column", LongType(), True),
            StructField("float_column", DoubleType(), True),
            StructField("double_column", DoubleType(), True),
            StructField("string_column", StringType(), True),
            StructField("boolean_column", BooleanType(), True),
            StructField("timestamp_column", TimestampType(TZ), True),
            StructField("timestampNTZ_column", TimestampType(NTZ), True),
            StructField("date_column", DateType(), True),
        ]
    )

    assert df.schema == expected_schema
    validate_telemetry_file_output(
        "test_dataframe_strategy_from_object_schema_generated_schema_telemetry.json"
    )


@given(data=st.data())
@settings(deadline=None, max_examples=5, suppress_health_check=list(HealthCheck))
def test_dataframe_strategy_from_object_schema_generated_values(
    data: st.DataObject, session: Session
):
    min_timestamp_value = datetime(
        2023, 1, 8, 19, 56, 47, 124971, tzinfo=ZoneInfo("UTC")
    )
    max_timestamp_value = datetime(
        2025, 12, 5, 20, 56, 47, 124971, tzinfo=ZoneInfo("UTC")
    )

    schema = pa.DataFrameSchema(
        {
            "BYTE_COLUMN": pa.Column(pa.Int8, checks=pa.Check.in_range(-128, 127)),
            "SHORT_COLUMN": pa.Column(pa.Int16, checks=pa.Check.in_range(-95, 100)),
            "INTEGER_COLUMN": pa.Column(pa.Int32, checks=pa.Check.in_range(-148, 138)),
            "LONG_COLUMN": pa.Column(
                pa.Int64, checks=pa.Check.in_range(-29777341365, 29631563833)
            ),
            "FLOAT_COLUMN": pa.Column(
                pa.Float32,
                checks=pa.Check.in_range(-2.7640867233276367, 3.1381418704986572),
            ),
            "DOUBLE_COLUMN": pa.Column(
                pa.Float64,
                checks=pa.Check.in_range(-2.6692257258090617, 2.5378806273606926),
            ),
            "STRING_COLUMN": pa.Column(
                pa.String,
                checks=pa.Check.str_matches(r"^[a-zA-Z0-9_-]+$"),
            ),
            "BOOLEAN_COLUMN": pa.Column(pa.Bool, checks=pa.Check.isin([True, False])),
            "TIMESTAMP_COLUMN": pa.Column(
                pd.DatetimeTZDtype(tz=ZoneInfo("UTC")),
                checks=pa.Check.in_range(
                    min_timestamp_value,
                    max_timestamp_value,
                ),
            ),
            "TIMESTAMPNTZ_COLUMN": pa.Column(
                pa.Timestamp,
                checks=pa.Check.in_range(
                    datetime(2020, 1, 1, 6, 29, 59, 768559),
                    datetime(2024, 11, 7, 14, 24, 40, 338141),
                ),
            ),
            "DATE_COLUMN": pa.Column(
                pa.Date, checks=pa.Check.in_range(date(2020, 1, 16), date(2024, 11, 1))
            ),
        }
    )

    strategy = dataframe_strategy(
        schema=schema,
        session=session,
    )

    snowpark_df = data.draw(strategy)
    pandas_df = snowpark_df.to_pandas()

    # Remove the TIMESTAMP_COLUMN from the expected schema because Pandera cannot validate it correctly.
    # Instead, we will validate the values manually.
    expected_schema = schema.remove_columns(["TIMESTAMP_COLUMN"])

    for column in expected_schema.columns.values():
        # Skip the validation of the data types. Just check the values
        column.dtype = None

    try:
        expected_schema.validate(pandas_df)
    except pa.errors.SchemaError as e:
        raise AssertionError(f"Pandera schema validation failed: {e.args[0]}.\n") from e

    out_of_range_timestamps_df = snowpark_df.filter(
        (col("TIMESTAMP_COLUMN") < min_timestamp_value)
        | (col("TIMESTAMP_COLUMN") > max_timestamp_value)
    )
    assert (
        out_of_range_timestamps_df.count() == 0
    ), f"TIMESTAMP_COLUMN values are out of range: {out_of_range_timestamps_df.collect()}"
    validate_telemetry_file_output(
        "test_dataframe_strategy_from_object_schema_generated_values_telemetry.json"
    )


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_from_object_schema_surrogate_characters(
    data: st.DataObject, session: Session
):
    schema = pa.DataFrameSchema({"string_column": pa.Column(pa.String)})

    with patch.object(
        schema,
        "strategy",
        return_value=st.just(
            pd.DataFrame(
                {"string_column": ["a", "\uD800", "c", "\uDC00", "e", "\uDFFF"]}
            )
        ),
    ):
        strategy = dataframe_strategy(
            schema=schema,
            session=session,
        )

        df = data.draw(strategy)

        assert df.collect() == [
            Row(STRING_COLUMN="a"),
            Row(STRING_COLUMN=" "),
            Row(STRING_COLUMN="c"),
            Row(STRING_COLUMN=" "),
            Row(STRING_COLUMN="e"),
            Row(STRING_COLUMN=" "),
        ]


@given(data=st.data())
@settings(deadline=None)
def test_dataframe_strategy_from_json_schema_surrogate_characters(
    data: st.DataObject, session: Session
):
    with patch(
        "snowflake.hypothesis_snowpark.strategies.load_json_schema",
        return_value={
            "pandera_schema": {
                "columns": {
                    "string_column": {
                        "dtype": "object",
                        "nullable": False,
                    }
                }
            },
            "custom_data": {
                "columns": [
                    {
                        "name": "string_column",
                        "type": "string",
                    }
                ]
            },
        },
    ), patch(
        "snowflake.hypothesis_snowpark.strategies.pa.DataFrameSchema.strategy",
        return_value=st.just(
            pd.DataFrame(
                {"string_column": ["a", "\uD800", "c", "\uDC00", "e", "\uDFFF"]}
            )
        ),
    ):
        strategy = dataframe_strategy(
            schema="schema.json",
            session=session,
        )

        df = data.draw(strategy)

        assert df.collect() == [
            Row(STRING_COLUMN="a"),
            Row(STRING_COLUMN=" "),
            Row(STRING_COLUMN="c"),
            Row(STRING_COLUMN=" "),
            Row(STRING_COLUMN="e"),
            Row(STRING_COLUMN=" "),
        ]


def get_expected(file_name: str) -> str:
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


def validate_telemetry_file_output(telemetry_file_name: str) -> None:
    telemetry_expected = get_expected(telemetry_file_name)
    telemetry_output = get_output_telemetry()

    telemetry_expected_obj = json.loads(telemetry_expected)
    telemetry_output_obj = json.loads(telemetry_output)
    exclude_telemetry_paths = [
        "root['timestamp']",
        "root['message']['metadata']['device_id']",
        "root['message']['metadata']",
        "root['message']['data']",
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
    assert isinstance(
        telemetry_output_obj.get("message")
        .get("metadata")
        .get("snowpark_checkpoints_version"),
        str,
    )

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json

from collections.abc import Generator
from pathlib import Path
import random

import pandas as pd
import pandera as pa
import pytest

from snowflake.hypothesis_snowpark.strategies_utils import (
    PYSPARK_TO_SNOWPARK_TYPES,
    apply_custom_null_values,
    generate_snowpark_dataframe,
    load_json_schema,
    pyspark_to_snowpark_type,
    temporary_random_seed,
)
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.types import (
    DataType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def test_load_json_schema_valid_file(tmp_path: Generator[Path, None, None]):
    json_content = {"key": "value"}
    json_file = tmp_path / "valid_file.json"
    json_file.write_text(json.dumps(json_content))
    result = load_json_schema(str(json_file))
    assert result == json_content


def test_load_json_schema_invalid_path():
    json_file = "invalid_path.json"
    with pytest.raises(ValueError, match=f"Invalid JSON schema path: {json_file}"):
        load_json_schema(json_file)


def test_load_json_schema_invalid_json(tmp_path: Generator[Path, None, None]):
    invalid_json_file = tmp_path / "invalid_json.json"
    invalid_json_file.write_text("{invalid_json}")
    with pytest.raises(ValueError, match="Error reading JSON schema file:"):
        load_json_schema(str(invalid_json_file))


def test_pyspark_to_snowpark_type_valid_types():
    for pyspark_type, expected_snowpark_type in PYSPARK_TO_SNOWPARK_TYPES.items():
        actual_snowpark_type = pyspark_to_snowpark_type(pyspark_type)
        assert isinstance(actual_snowpark_type, DataType)
        assert actual_snowpark_type == expected_snowpark_type


def test_pyspark_to_snowpark_type_invalid_types():
    pyspark_type = "unknown_type"
    with pytest.raises(
        ValueError, match=f"Unsupported PySpark data type: {pyspark_type}"
    ):
        pyspark_to_snowpark_type(pyspark_type)


def test_apply_custom_null_values_no_nulls():
    data = {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
    pandas_df = pd.DataFrame(data)
    custom_data = {
        "columns": [
            {"name": "col1", "rows_null_count": 0, "rows_count": 5},
            {"name": "col2", "rows_null_count": 0, "rows_count": 5},
        ]
    }
    result_df = apply_custom_null_values(pandas_df, custom_data)
    assert result_df.isnull().sum().sum() == 0


def test_apply_custom_null_values_some_nulls():
    data = {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
    pandas_df = pd.DataFrame(data)
    custom_data = {
        "columns": [
            {"name": "col1", "rows_null_count": 2, "rows_count": 5},
            {"name": "col2", "rows_null_count": 1, "rows_count": 5},
        ]
    }
    result_df = apply_custom_null_values(pandas_df, custom_data)
    assert result_df["col1"].isnull().sum() == 2
    assert result_df["col2"].isnull().sum() == 1


def test_apply_custom_null_values_all_nulls():
    data = {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
    pandas_df = pd.DataFrame(data)
    custom_data = {
        "columns": [
            {"name": "col1", "rows_null_count": 5, "rows_count": 5},
            {"name": "col2", "rows_null_count": 5, "rows_count": 5},
        ]
    }
    result_df = apply_custom_null_values(pandas_df, custom_data)
    assert result_df["col1"].isnull().sum() == 5
    assert result_df["col2"].isnull().sum() == 5


def test_apply_custom_null_values_invalid_column():
    data = {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
    pandas_df = pd.DataFrame(data)
    custom_data = {"columns": [{"name": "col3", "rows_null_count": 2, "rows_count": 5}]}
    result_df = apply_custom_null_values(pandas_df, custom_data)
    assert result_df["col1"].isnull().sum() == 0
    assert result_df["col2"].isnull().sum() == 0
    assert "col3" not in result_df.columns


def test_generate_snowpark_dataframe_valid_schema(local_session: Session):
    pandas_df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})

    pandera_schema = pa.DataFrameSchema(
        {
            "name": pa.Column(pa.String, nullable=False),
            "age": pa.Column(pa.Int, nullable=True),
        }
    )

    custom_data = {
        "columns": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "integer"},
        ]
    }

    snowpark_df = generate_snowpark_dataframe(
        pandas_df, local_session, pandera_schema, custom_data
    )

    expected_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), True),
        ]
    )

    assert isinstance(snowpark_df, DataFrame)
    assert snowpark_df.schema == expected_schema
    assert snowpark_df.collect() == [("Alice", 25), ("Bob", 30), ("Charlie", 35)]


def test_generate_snowpark_dataframe_missing_column_in_custom_data(
    local_session: Session,
):
    pandas_df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})

    pandera_schema = pa.DataFrameSchema(
        {
            "name": pa.Column(pa.String, nullable=False),
            "age": pa.Column(pa.Int, nullable=True),
        }
    )

    custom_data = {
        "columns": [
            {"name": "name", "type": "string"},
        ]
    }

    with pytest.raises(ValueError, match="Column 'age' is missing from custom_data"):
        generate_snowpark_dataframe(
            pandas_df, local_session, pandera_schema, custom_data
        )


def test_generate_snowpark_dataframe_missing_type_in_custom_data(
    local_session: Session,
):
    pandas_df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})

    pandera_schema = pa.DataFrameSchema(
        {
            "name": pa.Column(pa.String, nullable=False),
            "age": pa.Column(pa.Int, nullable=True),
        }
    )

    custom_data = {
        "columns": [
            {"name": "name", "type": "string"},
            {"name": "age"},
        ]
    }

    with pytest.raises(
        ValueError, match="Type for column 'age' is missing from custom_data"
    ):
        generate_snowpark_dataframe(
            pandas_df, local_session, pandera_schema, custom_data
        )


def test_generate_snowpark_dataframe_invalid_pyspark_type(local_session: Session):
    pandas_df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})

    pandera_schema = pa.DataFrameSchema(
        {
            "name": pa.Column(pa.String, nullable=False),
            "age": pa.Column(pa.Int, nullable=True),
        }
    )

    custom_data = {
        "columns": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "unknown_type"},
        ]
    }

    with pytest.raises(ValueError, match="Unsupported PySpark data type: unknown_type"):
        generate_snowpark_dataframe(
            pandas_df, local_session, pandera_schema, custom_data
        )


def test_temporary_random_seed_no_seed():
    initial_state = random.getstate()
    with temporary_random_seed():
        random_state_inside = random.getstate()
        assert random_state_inside != initial_state
    random_state_outside = random.getstate()
    assert random_state_outside == initial_state


def test_temporary_random_seed_with_seed():
    seed = 42
    initial_state = random.getstate()
    with temporary_random_seed(seed):
        random_state_inside = random.getstate()
        random.seed(seed)
        expected_state = random.getstate()
        assert random_state_inside == expected_state
    random_state_outside = random.getstate()
    assert random_state_outside == initial_state

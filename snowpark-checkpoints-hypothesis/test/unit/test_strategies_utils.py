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

import json
import random

from pathlib import Path

import numpy as np
import pandas as pd
import pandera as pa
import pytest

from snowflake.hypothesis_snowpark.strategies_utils import (
    apply_custom_null_values,
    generate_snowpark_dataframe,
    generate_snowpark_schema,
    load_json_schema,
    replace_surrogate_chars,
    temporary_random_seed,
)
from snowflake.snowpark import DataFrame, Row, Session
from snowflake.snowpark.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def test_load_json_schema_valid_file(tmp_path: Path):
    json_content = {"key": "value"}
    json_file = tmp_path / "valid_file.json"
    json_file.write_text(json.dumps(json_content))
    result = load_json_schema(str(json_file))
    assert result == json_content


def test_load_json_schema_invalid_path():
    json_file = "invalid_path.json"
    with pytest.raises(ValueError, match=f"Invalid JSON schema path: {json_file}"):
        load_json_schema(json_file)


def test_load_json_schema_invalid_json(tmp_path: Path):
    invalid_json_file = tmp_path / "invalid_json.json"
    invalid_json_file.write_text("{invalid_json}")
    with pytest.raises(ValueError, match="Error reading JSON schema file:"):
        load_json_schema(str(invalid_json_file))


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


def test_generate_snowpark_schema_no_custom_data():
    pandera_schema = pa.DataFrameSchema(
        {
            "name": pa.Column(pa.String, nullable=False),
            "age": pa.Column(pa.Int, nullable=True),
        }
    )

    expected_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", LongType(), True),
        ]
    )

    snowpark_schema = generate_snowpark_schema(pandera_schema)
    assert snowpark_schema == expected_schema


def test_generate_snowpark_schema_with_custom_data():
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

    expected_schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
        ]
    )

    snowpark_schema = generate_snowpark_schema(pandera_schema, custom_data)
    assert snowpark_schema == expected_schema


def test_generate_snowpark_schema_missing_column_in_custom_data():
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
        generate_snowpark_schema(pandera_schema, custom_data)


def test_generate_snowpark_schema_missing_type_in_custom_data():
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
        generate_snowpark_schema(pandera_schema, custom_data)


def test_generate_snowpark_schema_invalid_pyspark_type():
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

    with pytest.raises(TypeError, match="Unsupported PySpark data type: unknown_type"):
        generate_snowpark_schema(pandera_schema, custom_data)


def test_generate_snowpark_dataframe_with_valid_data(session: Session):
    pandas_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    snowpark_schema = StructType(
        [
            StructField("col1", IntegerType(), nullable=False),
            StructField("col2", StringType(), nullable=False),
        ]
    )
    result = generate_snowpark_dataframe(pandas_df, snowpark_schema, session)
    assert isinstance(result, DataFrame)
    assert result.collect() == [Row(COL1=1, COL2="a"), Row(COL1=2, COL2="b")]


def test_generate_snowpark_dataframe_with_empty_dataframe(session: Session):
    pandas_df = pd.DataFrame(columns=["col1", "col2"])
    snowpark_schema = StructType(
        [
            StructField("col1", IntegerType(), nullable=False),
            StructField("col2", StringType(), nullable=False),
        ]
    )
    result = generate_snowpark_dataframe(pandas_df, snowpark_schema, session)
    assert isinstance(result, DataFrame)
    assert result.count() == 0


def test_generate_snowpark_dataframe_with_null_values(session: Session):
    pandas_df = pd.DataFrame({"col1": [1, None], "col2": ["a", None]})
    snowpark_schema = StructType(
        [
            StructField("col1", IntegerType(), nullable=True),
            StructField("col2", StringType(), nullable=True),
        ]
    )
    result = generate_snowpark_dataframe(pandas_df, snowpark_schema, session)
    assert isinstance(result, DataFrame)
    assert result.filter(result["col1"].isNull()).count() == 1
    assert result.filter(result["col2"].isNull()).count() == 1


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


def test_replace_surrogate_chars_no_surrogates():
    pandas_df = pd.DataFrame({"col1": ["a", "b", "c"], "col2": ["d", "e", "f"]})
    columns = ["col1", "col2"]
    result_df = replace_surrogate_chars(pandas_df, columns)
    assert result_df.equals(pandas_df)


def test_replace_surrogate_chars_with_surrogates():
    pandas_df = pd.DataFrame(
        {
            "col1": ["a", "\uD800", "c"],
            "col2": ["d", "\uD950", "f"],
            "col3": ["g", "\uDFFF", "i"],
        }
    )
    columns = ["col1", "col2", "col3"]
    expected_df = pd.DataFrame(
        {"col1": ["a", " ", "c"], "col2": ["d", " ", "f"], "col3": ["g", " ", "i"]}
    )
    result_df = replace_surrogate_chars(pandas_df, columns)
    assert result_df.equals(expected_df)


def test_replace_surrogate_chars_partial_columns():
    pandas_df = pd.DataFrame(
        {"col1": ["a", "\uD800", "c"], "col2": ["d", "\uDFFF", "f"]}
    )
    columns = ["col1"]
    expected_df = pd.DataFrame({"col1": ["a", " ", "c"], "col2": ["d", "\uDFFF", "f"]})
    result_df = replace_surrogate_chars(pandas_df, columns)
    assert result_df.equals(expected_df)


def test_replace_surrogate_chars_empty_dataframe():
    columns = ["col1", "col2"]
    pandas_df = pd.DataFrame(columns=columns)
    result_df = replace_surrogate_chars(pandas_df, columns)
    assert result_df.equals(pandas_df)


def test_replace_surrogate_chars_non_string_columns():
    pandas_df = pd.DataFrame(
        {"col1": [1, 2, 3], "col2": ["d", "\uDFFF", "f"], "col3": [None, np.NaN, "x"]}
    )
    columns = ["col1", "col2", "col3"]
    expected_df = pd.DataFrame(
        {"col1": [1, 2, 3], "col2": ["d", " ", "f"], "col3": [None, np.NaN, "x"]}
    )
    result_df = replace_surrogate_chars(pandas_df, columns)
    assert result_df.equals(expected_df)

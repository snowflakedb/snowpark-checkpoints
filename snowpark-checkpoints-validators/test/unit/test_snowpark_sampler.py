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

from snowflake.snowpark_checkpoints.snowpark_sampler import (
    normalize_missing_values_pandas,
    to_pandas,
)
import pandas as pd
import numpy as np


class DummySnowparkDF:
    def __init__(self, pandas_df):
        self._pandas_df = pandas_df

    def toPandas(self):
        return self._pandas_df


def test_normalize_missing_values_pandas_integers():
    df = pd.DataFrame({"a": [1, None, 3], "b": [None, 2, 3]}, dtype="Int64")
    result = normalize_missing_values_pandas(df)
    assert result["a"].iloc[1] == 0
    assert result["b"].iloc[0] == 0


def test_normalize_missing_values_pandas_floats():
    df = pd.DataFrame({"a": [1.1, np.nan, 3.3], "b": [np.nan, 2.2, 3.3]})
    result = normalize_missing_values_pandas(df)
    assert result["a"].iloc[1] == 0.0
    assert result["b"].iloc[0] == 0.0


def test_normalize_missing_values_pandas_bools():
    df = pd.DataFrame(
        {"a": [True, None, False], "b": [None, False, True]}, dtype="boolean"
    )
    result = normalize_missing_values_pandas(df)
    assert result["a"].iloc[1] is np.False_
    assert result["b"].iloc[0] is np.False_


def test_normalize_missing_values_pandas_objects_and_strings():
    df = pd.DataFrame(
        {"a": ["foo", None, "bar"], "b": [None, "baz", "qux"]}, dtype="object"
    )
    result = normalize_missing_values_pandas(df)
    assert result["a"].iloc[1] == ""
    assert result["b"].iloc[0] == ""


def test_normalize_missing_values_pandas_mixed_types():  ###
    df = pd.DataFrame(
        {
            "int_col": pd.Series([1, None], dtype="Int64"),
            "float_col": [1.1, np.nan],
            "bool_col": pd.Series([True, None], dtype="boolean"),
            "str_col": ["a", None],
        }
    )
    result = normalize_missing_values_pandas(df)
    assert result["int_col"].iloc[1] == 0
    assert result["float_col"].iloc[1] == 0.0
    assert result["bool_col"].iloc[1] is np.False_
    assert result["str_col"].iloc[1] == ""


def test_to_pandas_calls_normalize(monkeypatch):
    df = pd.DataFrame({"a": [1, None], "b": [None, "foo"]})
    dummy_snowpark_df = DummySnowparkDF(df)
    called = {}

    def fake_normalize(df_arg):
        called["was_called"] = True
        return df_arg.fillna(0)

    monkeypatch.setattr(
        "snowflake.snowpark_checkpoints.snowpark_sampler.normalize_missing_values_pandas",
        fake_normalize,
    )

    result = to_pandas(dummy_snowpark_df)
    assert called.get("was_called", False)
    assert isinstance(result, pd.DataFrame)

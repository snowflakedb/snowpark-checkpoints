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

import pandas as pd
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    to_pandas,
    convert_all_to_utc_naive,
)
from snowflake.snowpark.types import (
    BinaryType,
    FloatType,
    StringType,
    TimestampType,
)
from snowflake.snowpark_checkpoints.utils.constants import (
    PANDAS_FLOAT_TYPE,
    PANDAS_LONG_TYPE,
    PANDAS_STRING_TYPE,
)


class DummyDataType:
    def __init__(self, name):
        self._name = name

    def typeName(self):
        return self._name


class DummyField:
    def __init__(self, name, datatype):
        self.name = name
        self.datatype = datatype
        self.typeName = datatype


class DummySchema:
    def __init__(self, fields):
        self.fields = fields


class DummySnowparkDF:
    def __init__(self, pandas_df, fields):
        self._pandas_df = pandas_df
        self.schema = DummySchema(fields)

    def toPandas(self):
        return self._pandas_df


def test_to_pandas_integer_conversion():
    df = pd.DataFrame({"int_col": [1, None]}, dtype="float")
    fields = [DummyField("int_col", DummyDataType("integer"))]
    sp_df = DummySnowparkDF(df, fields)

    result = to_pandas(sp_df)
    assert result["int_col"].dtype == PANDAS_LONG_TYPE
    assert result["int_col"].iloc[1] == 0


def test_to_pandas_string_and_binary_conversion():
    df = pd.DataFrame({"str_col": ["a", None], "bin_col": ["b", None]})
    fields = [
        DummyField("str_col", StringType()),
        DummyField("bin_col", BinaryType()),
    ]
    sp_df = DummySnowparkDF(df, fields)

    result = to_pandas(sp_df)
    assert result["str_col"].dtype == PANDAS_STRING_TYPE
    assert result["bin_col"].dtype == PANDAS_STRING_TYPE


def test_to_pandas_float_conversion():
    df = pd.DataFrame({"float_col": [1.1, None]}, dtype="float")
    fields = [DummyField("float_col", FloatType())]
    sp_df = DummySnowparkDF(df, fields)

    result = to_pandas(sp_df)
    assert result["float_col"].dtype == PANDAS_FLOAT_TYPE


def test_to_pandas_timestamp_conversion():
    utc_ts = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
    naive_ts = pd.Timestamp("2023-01-02 12:00:00")
    df = pd.DataFrame({"ts_col": [utc_ts, naive_ts]})
    fields = [DummyField("ts_col", TimestampType())]
    sp_df = DummySnowparkDF(df, fields)

    result = to_pandas(sp_df)
    assert pd.api.types.is_datetime64_any_dtype(result["ts_col"])
    assert result["ts_col"].iloc[0].tzinfo is None
    assert result["ts_col"].iloc[1].tzinfo is None


def test_convert_all_to_utc_naive_behavior():
    utc_ts = pd.Timestamp("2024-01-01 10:00:00", tz="UTC")
    naive_ts = pd.Timestamp("2024-01-01 12:00:00")
    none_val = pd.NaT
    series = pd.Series([utc_ts, naive_ts, none_val])

    result = convert_all_to_utc_naive(series)
    assert result[0].tzinfo is None
    assert result[1].tzinfo is None
    assert pd.isna(result[2])

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

from snowflake.snowpark_checkpoints.checkpoint import validate
from pandera import DataFrameSchema, Column, Check
import pandas as pd
import pytz


def test_pandera_validate_equivalent_dataframes():
    schema = DataFrameSchema(
        {
            "a": Column(
                int, checks=Check(lambda s: s > 0, element_wise=True), nullable=False
            )
        }
    )
    df = pd.DataFrame({"a": [1, 2, 3]})
    result, validated_df = validate(schema, df)
    assert result
    pd.testing.assert_frame_equal(validated_df, df)


def test_pandera_validate_object_vs_string():
    schema = DataFrameSchema({"a": Column(str, nullable=False)})

    df_object = pd.DataFrame({"a": pd.Series(["x", "y", "z"], dtype="object")})
    result, validated_df = validate(schema, df_object)
    assert result

    df_int_as_string = pd.DataFrame({"a": ["1", "2", "3"]})
    result, validated_df = validate(schema, df_int_as_string)
    assert result

    df_mixed = pd.DataFrame({"a": ["x", 1, "z"]})
    result, validated_df = validate(schema, df_mixed)
    assert not result


def test_pandera_validate_int_vs_string():
    schema = DataFrameSchema({"a": Column(int, nullable=False)})
    df_valid_int = pd.DataFrame({"a": [1, 2, 3]})
    result, _ = validate(schema, df_valid_int)
    assert result

    df_string_numbers = pd.DataFrame({"a": ["1", "2", "3"]})
    result, failure_cases = validate(schema, df_string_numbers)
    assert not result

    df_mixed = pd.DataFrame({"a": [1, "2", 3]})
    result, failure_cases = validate(schema, df_mixed)
    assert not result


def test_timestamp_ntz():
    schema = DataFrameSchema({"ts": Column(pd.Timestamp, nullable=False)})

    df = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-02 11:00", "2024-01-03 12:00"]
            )
        }
    )
    result, validated_df = validate(schema, df)
    assert result
    assert validated_df["ts"].dt.tz is None


def test_timestamp_utc_timezone():
    schema = DataFrameSchema({"ts": Column(pd.Timestamp, nullable=False)})

    df = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2024-01-01 10:00+00:00",
                    "2024-01-02 11:00+00:00",
                    "2024-01-03 12:00+00:00",
                ]
            )
        }
    )

    df["ts"] = df["ts"].dt.tz_convert("UTC").dt.tz_localize(None)

    result, validated_df = validate(schema, df)
    assert result
    assert validated_df["ts"].dt.tz is None


def convert_all_to_utc_naive(series: pd.Series) -> pd.Series:
    def convert(ts):
        if ts.tz is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert("UTC").tz_localize(None)

    return series.apply(convert)


def test_timestamp_mixed_timezones_fails():
    schema = DataFrameSchema({"ts": Column(pd.Timestamp, nullable=False)})
    eastern = pytz.timezone("US/Eastern")
    df = pd.DataFrame(
        {
            "ts": [
                pd.Timestamp("2024-01-01 10:00"),
                eastern.localize(pd.Timestamp("2024-01-02 11:00")),
                pd.Timestamp("2024-01-03 12:00+00:00"),
            ]
        }
    )

    df["ts"] = convert_all_to_utc_naive(df["ts"])
    result, validated_df = validate(schema, df)

    assert result
    assert validated_df["ts"].dt.tz is None

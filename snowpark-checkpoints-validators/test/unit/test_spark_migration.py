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
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pytest import fixture
from snowflake.snowpark import Session

from snowflake.snowpark_checkpoints.spark_migration import compare_spark_snowpark_dfs


@fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@fixture
def snowpark_session():
    return Session.builder.config("local_testing", True).getOrCreate()


@fixture
def data():
    return [("Raul", 21), ("John", 34), ("Rose", 50)]


def test_compare_spark_snowpark_dfs_same_columns(spark_session, snowpark_session, data):
    spark_df = spark_session.createDataFrame(data, schema="name string, age integer")
    snowpark_df = snowpark_session.create_dataframe(data, schema=["name", "age"])
    cmp = compare_spark_snowpark_dfs(spark_df, snowpark_df)
    assert cmp.empty


def test_compare_spark_snowpark_dfs_different_column_names(
    spark_session, snowpark_session, data
):
    spark_df = spark_session.createDataFrame(data, schema="name string, age integer")
    snowpark_df = snowpark_session.create_dataframe(data, schema=["full_name", "age"])
    expected_data = [
        ("Raul", None, "spark_only"),
        ("John", None, "spark_only"),
        ("Rose", None, "spark_only"),
        (None, "Raul", "snowpark_only"),
        (None, "John", "snowpark_only"),
        (None, "Rose", "snowpark_only"),
    ]

    expected_cmp = pd.DataFrame.from_records(
        expected_data, columns=["NAME", "FULL_NAME", "_merge"]
    )
    cmp = compare_spark_snowpark_dfs(spark_df, snowpark_df)
    assert_frame_equal(expected_cmp, cmp, check_dtype=False, check_categorical=False)


def test_compare_spark_snowpark_dfs_column_left_mismatch(
    spark_session, snowpark_session, data
):
    spark_df = spark_session.createDataFrame(data, schema="name string, age integer")
    spark_df = spark_df.drop("age")
    snowpark_df = snowpark_session.create_dataframe(data, schema=["name", "age"])

    expected_cmp = pd.DataFrame.from_records(data, columns=["NAME", "AGE"])
    expected_cmp = expected_cmp.drop(columns="NAME")
    expected_cmp["_merge"] = pd.Categorical(["snowpark_only"] * 3)
    cmp = compare_spark_snowpark_dfs(spark_df, snowpark_df)
    assert_frame_equal(expected_cmp, cmp, check_dtype=False, check_categorical=False)


def test_compare_spark_snowpark_dfs_column_right_mismatch(
    spark_session, snowpark_session, data
):
    spark_df = spark_session.createDataFrame(data, schema="name string, age integer")
    snowpark_df = snowpark_session.create_dataframe(data, schema=["name", "age"])
    snowpark_df = snowpark_df.drop("age")

    expected_cmp = pd.DataFrame.from_records(data, columns=["NAME", "AGE"])
    expected_cmp = expected_cmp.drop(columns="NAME")
    expected_cmp["_merge"] = pd.Categorical(["spark_only"] * 3)
    cmp = compare_spark_snowpark_dfs(spark_df, snowpark_df)
    assert_frame_equal(expected_cmp, cmp, check_dtype=False, check_categorical=False)


def test_compare_spark_snowpark_dfs_right_more_rows(
    spark_session, snowpark_session, data
):
    spark_df = spark_session.createDataFrame(data, schema="name string, age integer")
    spark_df = spark_df.filter(spark_df.age < 50)
    snowpark_df = snowpark_session.create_dataframe(data, schema=["name", "age"])

    expected_cmp = pd.DataFrame.from_records(data, columns=["NAME", "AGE"])
    expected_cmp = expected_cmp[expected_cmp["AGE"] >= 50]
    expected_cmp["_merge"] = pd.Categorical(["snowpark_only"])
    cmp = compare_spark_snowpark_dfs(spark_df, snowpark_df)
    assert_frame_equal(expected_cmp, cmp, check_dtype=False, check_categorical=False)


def test_compare_spark_snowpark_dfs_left_more_rows(
    spark_session, snowpark_session, data
):
    spark_df = spark_session.createDataFrame(data, schema="name string, age integer")
    snowpark_df = snowpark_session.create_dataframe(data, schema=["name", "age"])
    snowpark_df = snowpark_df.filter(snowpark_df.age < 50)

    expected_cmp = pd.DataFrame.from_records(data, columns=["NAME", "AGE"])
    expected_cmp = expected_cmp[expected_cmp["AGE"] >= 50]
    expected_cmp["_merge"] = pd.Categorical(["spark_only"])
    cmp = compare_spark_snowpark_dfs(spark_df, snowpark_df)
    assert_frame_equal(expected_cmp, cmp, check_dtype=False, check_categorical=False)


def test_compare_spark_snowpark_dfs_different_rows(spark_session, snowpark_session):
    spark_data = [("John", 34), ("Raul", 21)]
    snowpark_data = [("Rose", 50)]
    spark_df = spark_session.createDataFrame(
        spark_data, schema="name string, age integer"
    )
    spark_df = spark_df.filter(spark_df.age < 50)
    snowpark_df = snowpark_session.create_dataframe(
        snowpark_data, schema=["name", "age"]
    )
    snowpark_df = snowpark_df.filter(snowpark_df.age >= 50)

    original_df = pd.DataFrame.from_records(
        spark_data + snowpark_data, columns=["NAME", "AGE"]
    )
    snowpark_cmp = original_df[original_df["AGE"] >= 50]
    snowpark_cmp["_merge"] = pd.Categorical(["snowpark_only"])
    spark_cmp = original_df[original_df["AGE"] < 50]
    spark_cmp["_merge"] = pd.Categorical(["spark_only"] * 2)

    expected_df = pd.concat([spark_cmp, snowpark_cmp], ignore_index=True)

    cmp = compare_spark_snowpark_dfs(spark_df, snowpark_df)
    assert_frame_equal(
        expected_df,
        cmp,
        check_dtype=False,
        check_categorical=False,
    )

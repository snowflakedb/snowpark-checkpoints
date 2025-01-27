#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from pyspark.sql import DataFrame as SparkDataFrame
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark import Session
from pyspark.sql import SparkSession
import pytest
import pandas as pd
import numpy as np
import os
from pathlib import Path
import tempfile
from snowflake.snowpark_checkpoints.utils.telemetry import (
    get_telemetry_manager,
)
from telemetry_compare_utils import validate_telemetry_file_output

TELEMETRY_FOLDER = "telemetry"


@pytest.fixture(scope="function")
def telemetry_output_path():
    folder = os.urandom(8).hex()
    directory = Path(tempfile.gettempdir()).resolve() / folder
    os.makedirs(directory)
    telemetry_dir = directory / TELEMETRY_FOLDER

    telemetry_manager = get_telemetry_manager()
    telemetry_manager.set_sc_output_path(telemetry_dir)
    return str(telemetry_dir)


@pytest.fixture
def job_context():
    session = Session.builder.getOrCreate()
    spark_session = SparkSession.builder.getOrCreate()
    return SnowparkJobContext(session, spark_session, "test job", True)


def test_spark_checkpoint_scalar_passing(job_context, telemetry_output_path):
    def my_spark_scalar_fn(df: SparkDataFrame):
        return df.count()

    @check_with_spark(
        job_context=job_context,
        spark_function=my_spark_scalar_fn,
        checkpoint_name="test_spark_scalar_fn",
    )
    def my_snowpark_scalar_fn(df: SnowparkDataFrame):
        return df.count()

    df = job_context.snowpark_session.create_dataframe(
        [[1, 2], [3, 4]], schema=["a", "b"]
    )
    count = my_snowpark_scalar_fn(df)
    assert count == 2
    validate_telemetry_file_output(
        "spark_checkpoint_scalar_passing_telemetry.json", telemetry_output_path
    )


def test_spark_checkpoint_scalar_fail(job_context, telemetry_output_path):
    def my_spark_scalar_fn(df: SparkDataFrame):
        return df.count() + 1

    @check_with_spark(
        job_context=job_context,
        spark_function=my_spark_scalar_fn,
        checkpoint_name="test_spark_scalar_fn",
    )
    def my_snowpark_scalar_fn(df: SnowparkDataFrame):
        return df.count()

    df = job_context.snowpark_session.create_dataframe(
        [[1, 2], [3, 4]], schema=["a", "b"]
    )
    try:
        my_snowpark_scalar_fn(df)
        assert (False, "Should have failed")
    except:
        pass
    validate_telemetry_file_output(
        "spark_checkpoint_scalar_fail_telemetry.json", telemetry_output_path
    )


def test_spark_checkpoint_df_pass(job_context, telemetry_output_path):
    def my_spark_fn(df: SparkDataFrame):
        return df.filter(df.A > 2)

    @check_with_spark(
        job_context=job_context,
        spark_function=my_spark_fn,
        checkpoint_name="test_spark_scalar_fn",
    )
    def my_snowpark_fn(df: SnowparkDataFrame):
        return df.filter(df.a > 2)

    df = job_context.snowpark_session.create_dataframe(
        [[1, 2], [3, 4]], schema=["a", "b"]
    )
    my_snowpark_fn(df)
    validate_telemetry_file_output(
        "spark_checkpoint_df_pass_telemetry.json", telemetry_output_path
    )


def test_spark_checkpoint_df_fail(job_context, telemetry_output_path):
    def my_spark_fn(df: SparkDataFrame):
        return df.filter(df.A > 2)

    @check_with_spark(
        job_context=job_context,
        spark_function=my_spark_fn,
        checkpoint_name="test_spark_scalar_fn",
    )
    def my_snowpark_fn(df: SnowparkDataFrame):
        return df.filter(df.a < 2)

    df = job_context.snowpark_session.create_dataframe(
        [[1, 2], [3, 4]], schema=["a", "b"]
    )
    try:
        my_snowpark_fn(df)
        assert (False, "Should have failed")
    except:
        pass
    validate_telemetry_file_output(
        "spark_checkpoint_df_fail_telemetry.json", telemetry_output_path
    )


def test_spark_checkpoint_limit_sample(job_context, telemetry_output_path):
    def my_spark_fn(df: SparkDataFrame):
        return df.filter(df.A < 50)

    @check_with_spark(
        job_context=job_context,
        spark_function=my_spark_fn,
        checkpoint_name="test_spark_scalar_fn",
        sample_number=10,
        sampling_strategy=SamplingStrategy.LIMIT,
    )
    def my_snowpark_fn(df: SnowparkDataFrame):
        return df.filter(df.a < 50)

    data_df = pd.DataFrame(
        np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
    )
    df = job_context.snowpark_session.create_dataframe(data_df)
    my_snowpark_fn(df)
    validate_telemetry_file_output(
        "spark_checkpoint_limit_sample_telemetry.json", telemetry_output_path
    )


def test_spark_checkpoint_random_sample(job_context, telemetry_output_path):
    def my_spark_fn(df: SparkDataFrame):
        return df.filter(df.A < 50)

    @check_with_spark(
        job_context=job_context,
        spark_function=my_spark_fn,
        checkpoint_name="test_spark_scalar_fn",
        sample_number=10,
        sampling_strategy=SamplingStrategy.RANDOM_SAMPLE,
    )
    def my_snowpark_fn(df: SnowparkDataFrame):
        return df.filter(df.a < 50)

    data_df = pd.DataFrame(
        np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")
    )
    df = job_context.snowpark_session.create_dataframe(data_df)
    my_snowpark_fn(df)
    validate_telemetry_file_output(
        "spark_checkpoint_random_sample_telemetry.json", telemetry_output_path
    )

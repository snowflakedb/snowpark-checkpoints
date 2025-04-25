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

import glob
import logging
import os
import tempfile
import time

from datetime import datetime
from pathlib import Path

import pyspark.sql.types as t
import pytest

from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.types import BooleanType, LongType, StructField, StructType
from pytest import fixture

from snowflake.snowpark.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from snowflake.snowpark_checkpoints_collector import (
    collect_dataframe_checkpoint,
)
from snowflake.snowpark_checkpoints_collector.collection_common import (
    DOT_PARQUET_EXTENSION,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    CheckpointMode,
)
from snowflake.snowpark_checkpoints_collector.singleton import Singleton
from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)
from snowflake.snowpark_checkpoints_collector.summary_stats_collector import (
    generate_parquet_for_spark_df,
)
from snowflake.snowpark_checkpoints_collector.utils.telemetry import (
    get_telemetry_manager,
)
from snowflake.snowpark_checkpoints_collector.io_utils.io_default_strategy import (
    IODefaultStrategy,
)
from snowflake.snowpark_checkpoints_collector.io_utils.io_file_manager import (
    get_io_file_manager,
)
from unittest.mock import patch

import inspect
from telemetry_compare_utils import validate_telemetry_file_output, reset_telemetry_util


TEST_COLLECT_DF_MODE_2_EXPECTED_DIRECTORY_NAME = "test_collect_df_mode_2_expected"
TELEMETRY_FOLDER = "telemetry"
TOP_LEVEL_LOGGER_NAME = "snowflake.snowpark_checkpoints_collector"


@pytest.fixture(scope="function")
def telemetry_output():
    temp_dir = Path(tempfile.gettempdir()).resolve()
    telemetry_manager = get_telemetry_manager()
    telemetry_output_path = temp_dir / TELEMETRY_FOLDER
    telemetry_manager.set_sc_output_path(telemetry_output_path)
    return telemetry_output_path


@pytest.fixture(scope="function")
def test_id():
    return int(time.time())


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture(autouse=True)
def singleton():
    Singleton._instances = {}


@fixture
def spark_schema():
    return t.StructType(
        [
            t.StructField("BYTE", t.ByteType(), True),
            t.StructField("SHORT", t.ShortType(), True),
            t.StructField("INTEGER", t.IntegerType(), True),
            t.StructField("LONG", t.LongType(), True),
            t.StructField("FLOAT", t.FloatType(), True),
            t.StructField("DOUBLE", t.DoubleType(), True),
            # StructField("decimal", DecimalType(10, 3), True),
            t.StructField("STRING", t.StringType(), True),
            # StructField("binary", BinaryType(), True),
            t.StructField("BOOLEAN", t.BooleanType(), True),
            t.StructField("DATE", t.DateType(), True),
            # StructField("timestamp", TimestampType(), True),
            # StructField("timestamp_ntz", TimestampType(), True),
        ]
    )


@fixture
def snowpark_schema():
    return StructType(
        [
            StructField("BYTE", LongType(), True),
            StructField("SHORT", LongType(), True),
            StructField("INTEGER", LongType(), True),
            StructField("LONG", LongType(), True),
            StructField("FLOAT", DoubleType(), True),
            StructField("DOUBLE", DoubleType(), True),
            # StructField("decimal", DecimalType(10, 3), True),
            StructField("STRING", StringType(), True),
            # StructField("binary", BinaryType(), True),
            StructField("BOOLEAN", BooleanType(), True),
            StructField("DATE", DateType(), True),
            # StructField("timestamp", TimestampType(), True),
            # StructField("timestamp_ntz", TimestampType(), True),
        ]
    )


@fixture
def data():
    date_format = "%Y-%m-%d"
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    timestamp_ntz_format = "%Y-%m-%d %H:%M:%S"

    return [
        [
            3,
            789,
            13579,
            1231231231,
            7.8,
            2.345678,
            # Decimal(7.891),
            "red",
            # b"info",
            True,
            datetime.strptime("2023-03-01", date_format),
            # datetime.strptime("2023-03-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-03-01 12:00:00", timestamp_ntz_format),
        ],
        [
            4,
            101,
            24680,
            3213213210,
            0.12,
            3.456789,
            # Decimal(0.123),
            "red",
            # b"test",
            False,
            datetime.strptime("2023-04-01", date_format),
            # datetime.strptime("2023-04-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-04-01 12:00:00", timestamp_ntz_format),
        ],
        [
            5,
            202,
            36912,
            4564564560,
            3.45,
            4.567890,
            # Decimal(3.456),
            "red",
            # b"example2",
            True,
            datetime.strptime("2023-05-01", date_format),
            # datetime.strptime("2023-05-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-05-01 12:00:00", timestamp_ntz_format),
        ],
        [
            6,
            303,
            48123,
            7897897890,
            6.78,
            5.678901,
            # Decimal(6.789),
            "red",
            # b"sample2",
            False,
            datetime.strptime("2023-06-01", date_format),
            # datetime.strptime("2023-06-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-06-01 12:00:00", timestamp_ntz_format),
        ],
        [
            7,
            404,
            59234,
            9879879870,
            9.01,
            6.789012,
            # Decimal(9.012),
            "red",
            # b"data2",
            True,
            datetime.strptime("2023-07-01", date_format),
            # datetime.strptime("2023-07-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-07-01 12:00:00", timestamp_ntz_format),
        ],
        [
            8,
            505,
            70345,
            1231231234,
            1.23,
            7.890123,
            # Decimal(1.234),
            "blue",
            # b"test2",
            False,
            datetime.strptime("2023-08-01", date_format),
            # datetime.strptime("2023-08-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-08-01 12:00:00", timestamp_ntz_format),
        ],
        [
            9,
            606,
            81456,
            3213213214,
            4.56,
            8.901234,
            # Decimal(4.567),
            "blue",
            # b"example3",
            True,
            datetime.strptime("2023-09-01", date_format),
            # datetime.strptime("2023-09-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-09-01 12:00:00", timestamp_ntz_format),
        ],
        [
            10,
            707,
            92567,
            4564564564,
            7.8,
            9.012345,
            # Decimal(7.892),
            "blue",
            # b"sample3",
            False,
            datetime.strptime("2023-10-01", date_format),
            # datetime.strptime("2023-10-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-10-01 12:00:00", timestamp_ntz_format),
        ],
        [
            11,
            808,
            103678,
            7897897894,
            0.12,
            0.123456,
            # Decimal(0.123),
            "green",
            # b"data3",
            True,
            datetime.strptime("2023-11-01", date_format),
            # datetime.strptime("2023-11-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-11-01 12:00:00", timestamp_ntz_format),
        ],
        [
            12,
            909,
            114789,
            9879879874,
            3.45,
            1.234567,
            # Decimal(3.456),
            "green",
            # b"test3",
            False,
            datetime.strptime("2023-12-01", date_format),
            # datetime.strptime("2023-12-01 12:00:00", timestamp_format),
            # datetime.strptime("2023-12-01 12:00:00", timestamp_ntz_format),
        ],
    ]


def test_collect_checkpoint_mode_2_parquet_directory(
    spark_session,
    data,
    spark_schema,
    snowpark_schema,
    test_id,
    telemetry_output,
):
    checkpoint_name = f"test_collect_checkpoint_mode_2_{test_id}"

    pyspark_df = spark_session.createDataFrame(data, schema=spark_schema).orderBy(
        "INTEGER"
    )

    temp_dir = Path(tempfile.gettempdir()).resolve()
    output_path = os.path.join(temp_dir, checkpoint_name)

    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        mode=CheckpointMode.DATAFRAME,
        output_path=output_path,
    )

    parquet_directory = os.path.join(
        output_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME, checkpoint_name
    )

    assert os.path.exists(parquet_directory)
    parquet_files_first = glob.glob(
        os.path.join(parquet_directory, f"*{DOT_PARQUET_EXTENSION}")
    )

    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        mode=CheckpointMode.DATAFRAME,
        output_path=output_path,
    )

    parquet_files_second = glob.glob(
        os.path.join(parquet_directory, f"*{DOT_PARQUET_EXTENSION}")
    )
    assert parquet_files_first != parquet_files_second
    validate_telemetry(
        "test_collect_checkpoint_mode_2_parquet_directory ", telemetry_output
    )


def test_collect_checkpoint_mode_2(
    spark_session,
    data,
    spark_schema,
    snowpark_schema,
    test_id,
    telemetry_output,
):
    checkpoint_name = f"test_collect_checkpoint_mode_2_{test_id}"

    pyspark_df = spark_session.createDataFrame(data, schema=spark_schema).orderBy(
        "INTEGER"
    )

    temp_dir = tempfile.gettempdir()
    output_path = os.path.join(temp_dir, checkpoint_name)

    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        mode=CheckpointMode.DATAFRAME,
        output_path=output_path,
    )

    validate_dataframes(checkpoint_name, pyspark_df, snowpark_schema)
    validate_telemetry("test_collect_checkpoint_mode_2", telemetry_output)


def test_collect_empty_dataframe_with_schema(
    spark_session, spark_schema, snowpark_schema, telemetry_output
):
    checkpoint_name = "test_collect_empty_dataframe_with_schema"

    data = []
    pyspark_df = spark_session.createDataFrame(data=data, schema=spark_schema)
    expected_df = pyspark_df.toPandas()
    expected_df.columns = expected_df.columns.str.upper()

    collect_dataframe_checkpoint(
        pyspark_df, checkpoint_name=checkpoint_name, mode=CheckpointMode.DATAFRAME
    )

    validate_dataframes(checkpoint_name, pyspark_df, snowpark_schema)
    validate_telemetry("test_collect_empty_dataframe_with_schema", telemetry_output)


def test_collect_invalid_mode(
    spark_session: SparkSession,
    data: list[list],
    spark_schema: t.StructType,
    telemetry_output: Path,
    caplog: pytest.LogCaptureFixture,
):
    pyspark_df = spark_session.createDataFrame(data=data, schema=spark_schema)
    checkpoint_mode = 3
    expected_error_msg = f"Invalid mode value: {checkpoint_mode}"

    with pytest.raises(Exception) as ex_info, caplog.at_level(
        level=logging.ERROR, logger=TOP_LEVEL_LOGGER_NAME
    ):
        collect_dataframe_checkpoint(
            pyspark_df, checkpoint_name="invalid_mode", mode=checkpoint_mode
        )

    assert expected_error_msg == str(ex_info.value)
    assert expected_error_msg in caplog.text


def test_generate_parquet_for_spark_df(data, spark_schema, test_id, telemetry_output):
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(data, schema=spark_schema)
    parquet_directory = os.path.join(
        tempfile.gettempdir(),
        f"test_generate_parquet_for_spark_df_{test_id}",
    )

    generate_parquet_for_spark_df(spark_df, parquet_directory)
    target_dir = os.path.join(parquet_directory, "**", f"*{DOT_PARQUET_EXTENSION}")
    files = glob.glob(target_dir, recursive=True)
    assert len(files) > 0


def test_spark_df_mode_dataframe(
    spark_schema, snowpark_schema, data, test_id, telemetry_output
):
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(data, schema=spark_schema)
    checkpoint_name = f"test_spark_df_mode_dataframe_{test_id}"
    parquet_directory = os.path.join(
        tempfile.gettempdir(),
        checkpoint_name,
    )

    generate_parquet_for_spark_df(spark_df, parquet_directory)

    snow = SnowConnection()
    snow.create_snowflake_table_from_local_parquet(checkpoint_name, parquet_directory)

    validate_dataframes(checkpoint_name, spark_df, snowpark_schema)


def test_io_strategy(
    spark_session: SparkSession,
    data: list[list],
    spark_schema: t.StructType,
    snowpark_schema: StructType,
    singleton: None,
    test_id: int,
    telemetry_output: Path,
):
    try:
        checkpoint_name = f"test_io_strategy_{test_id}"

        class TestStrategy(IODefaultStrategy):
            pass

        number_of_methods = inspect.getmembers(
            IODefaultStrategy, predicate=inspect.isfunction
        )
        strategy = TestStrategy()
        get_io_file_manager().set_strategy(strategy)

        with patch.object(
            strategy, "getcwd", wraps=strategy.getcwd
        ) as getcwd_spy, patch.object(
            strategy, "ls", wraps=strategy.ls
        ) as ls_spy, patch.object(
            strategy, "mkdir", wraps=strategy.mkdir
        ) as mkdir_spy, patch.object(
            strategy, "write", wraps=strategy.write
        ) as write_spy, patch.object(
            strategy, "read", wraps=strategy.read
        ) as read_spy, patch.object(
            strategy, "read_bytes", wraps=strategy.read_bytes
        ) as read_bytes_spy, patch.object(
            strategy, "file_exists", wraps=strategy.file_exists
        ) as file_exists_spy, patch.object(
            strategy, "folder_exists", wraps=strategy.folder_exists
        ) as folder_exists_spy, patch.object(
            strategy, "remove_dir", wraps=strategy.remove_dir
        ) as remove_dir_spy:
            telemetry_manager = reset_telemetry_util()
            telemetry_manager.set_sc_output_path(Path(telemetry_output))
            pyspark_df = spark_session.createDataFrame(
                data, schema=spark_schema
            ).orderBy("INTEGER")

            temp_dir = tempfile.gettempdir()
            output_path = os.path.join(temp_dir, checkpoint_name)
            output_parquet_path = os.path.join(
                output_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME, checkpoint_name
            )
            if not os.path.exists(output_parquet_path):
                os.makedirs(output_parquet_path)

            collect_dataframe_checkpoint(
                pyspark_df,
                checkpoint_name=checkpoint_name,
                mode=CheckpointMode.DATAFRAME,
                output_path=output_path,
            )

            # Assert
            assert len(number_of_methods) == 10
            assert getcwd_spy.call_count == 3
            assert mkdir_spy.call_count == 5
            assert write_spy.call_count == 2
            assert read_spy.call_count == 1
            assert read_bytes_spy.call_count == 1
            assert file_exists_spy.call_count == 2
            assert ls_spy.call_count == 3
            assert folder_exists_spy.call_count == 1
            assert remove_dir_spy.call_count == 1
            validate_dataframes(checkpoint_name, pyspark_df, snowpark_schema)
    finally:
        get_io_file_manager().set_strategy(IODefaultStrategy())


def validate_dataframes(
    checkpoint_name: str, pyspark_df: SparkDataFrame, expected_schema: t.StructType
) -> None:
    expected_df = pyspark_df.toPandas()
    expected_df.columns = expected_df.columns.str.upper()

    snow_connection = SnowConnection()
    snowpark_df = snow_connection.session.read.table(checkpoint_name).orderBy("INTEGER")
    actual_df = snowpark_df.toPandas()
    actual_df.columns = actual_df.columns.str.upper()

    assert expected_schema == snowpark_df.schema
    assert_frame_equal(
        expected_df, actual_df, check_dtype=False, check_categorical=False
    )


def validate_telemetry(checkpoint_name: str, telemetry_output: Path) -> None:
    telemetry_file_name = f"{checkpoint_name}_telemetry.json"
    validate_telemetry_file_output(
        telemetry_file_name=telemetry_file_name,
        output_path=telemetry_output,
        telemetry_expected_folder=TEST_COLLECT_DF_MODE_2_EXPECTED_DIRECTORY_NAME,
    )

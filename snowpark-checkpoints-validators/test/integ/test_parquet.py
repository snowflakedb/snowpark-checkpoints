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

import logging
import os
import tempfile

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pyspark.sql import SparkSession
from pytest import fixture, raises
from telemetry_compare_utils import validate_telemetry_file_output

from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from snowflake.snowpark_checkpoints.checkpoint import validate_dataframe_checkpoint
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.utils.constants import (
    DATAFRAME_EXECUTION_MODE,
    FAIL_STATUS,
    PASS_STATUS,
    CheckpointMode,
)
from snowflake.snowpark_checkpoints.utils.telemetry import (
    get_telemetry_manager,
)


TELEMETRY_FOLDER = "telemetry"
LOGGER_NAME = "snowflake.snowpark_checkpoints.checkpoint"


@fixture(scope="function")
def telemetry_output_path():
    folder = os.urandom(8).hex()
    directory = Path(tempfile.gettempdir()).resolve() / folder
    os.makedirs(directory)
    telemetry_dir = directory / TELEMETRY_FOLDER

    telemetry_manager = get_telemetry_manager()
    telemetry_manager.set_sc_output_path(telemetry_dir)
    return str(telemetry_dir)


@fixture
def job_context():
    session = Session.builder.getOrCreate()
    job_context = SnowparkJobContext(
        session, SparkSession.builder.getOrCreate(), "real_demo", True
    )
    return job_context


@fixture
def spark_schema():
    import pyspark.sql.types as t

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


def test_df_mode_dataframe(job_context, snowpark_schema, data, telemetry_output_path):
    stage_name = "test_df_mode_dataframe"
    checkpoint_name = "test_mode_dataframe_checkpoint"
    df = job_context.snowpark_session.create_dataframe(data, snowpark_schema)
    df.write.save_as_table(checkpoint_name, mode="overwrite")

    mocked_session = MagicMock()
    job_context._mark_pass = mocked_session

    with patch(
        "snowflake.snowpark_checkpoints.utils.utils_checks._update_validation_result"
    ) as mocked_update:
        validate_dataframe_checkpoint(
            df,
            checkpoint_name,
            job_context=job_context,
            mode=CheckpointMode.DATAFRAME,
        )

    mocked_update.assert_called_once_with(checkpoint_name, PASS_STATUS, None)
    mocked_session.assert_called_once_with(checkpoint_name, DATAFRAME_EXECUTION_MODE)
    validate_telemetry_file_output(
        "df_mode_dataframe_telemetry.json", telemetry_output_path
    )


def test_df_mode_dataframe_mismatch(
    job_context: SnowparkJobContext,
    snowpark_schema: StructType,
    data: list[list],
    telemetry_output_path: str,
    caplog: pytest.LogCaptureFixture,
):
    checkpoint_name = "test_mode_dataframe_checkpoint_fail"

    data_copy = data.copy()
    df = job_context.snowpark_session.create_dataframe(data_copy, snowpark_schema)
    df.write.save_as_table(checkpoint_name, mode="overwrite")

    data.pop()
    df_spark = job_context.snowpark_session.create_dataframe(data, snowpark_schema)

    with patch(
        "snowflake.snowpark_checkpoints.utils.utils_checks._update_validation_result"
    ) as mocked_update, raises(
        SchemaValidationError,
        match=f"Data mismatch for checkpoint {checkpoint_name}",
    ) as ex, caplog.at_level(
        level=logging.ERROR, logger=LOGGER_NAME
    ):
        validate_dataframe_checkpoint(
            df_spark,
            checkpoint_name,
            job_context=job_context,
            mode=CheckpointMode.DATAFRAME,
        )

    mocked_update.assert_called_once_with(checkpoint_name, FAIL_STATUS, None)
    validate_telemetry_file_output(
        "df_mode_dataframe_mismatch_telemetry.json", telemetry_output_path
    )
    assert str(ex.value) in caplog.text


def test_df_mode_dataframe_job_none(
    job_context: SnowparkJobContext,
    snowpark_schema: StructType,
    data: list[list],
    caplog: pytest.LogCaptureFixture,
):
    checkpoint_name = "test_mode_dataframe_checkpoint_fail"
    df_spark = job_context.snowpark_session.create_dataframe(data, snowpark_schema)

    with raises(
        ValueError,
        match="No job context provided. Please provide one when using DataFrame mode validation.",
    ) as ex, caplog.at_level(level=logging.ERROR, logger=LOGGER_NAME):
        validate_dataframe_checkpoint(
            df_spark,
            checkpoint_name,
            job_context=None,
            mode=CheckpointMode.DATAFRAME,
        )
    assert str(ex.value) in caplog.text


def test_df_mode_dataframe_invalid_mode(
    job_context: SnowparkJobContext,
    snowpark_schema: StructType,
    data: list[list],
    caplog: pytest.LogCaptureFixture,
):
    checkpoint_name = "test_mode_dataframe_checkpoint_fail"
    df_spark = job_context.snowpark_session.create_dataframe(data, snowpark_schema)

    with raises(
        ValueError,
        match=(
            "Invalid validation mode. "
            "Please use 1 for schema validation or 2 for full data validation."
        ),
    ) as ex, caplog.at_level(level=logging.ERROR, logger=LOGGER_NAME):
        validate_dataframe_checkpoint(
            df_spark,
            checkpoint_name,
            job_context=job_context,
            mode="invalid",
        )
    assert str(ex.value) in caplog.text


@patch("snowflake.snowpark_checkpoints.checkpoint.is_checkpoint_enabled")
def test_validate_dataframe_checkpoint_disabled_checkpoint(
    mock_is_checkpoint_enabled: MagicMock, caplog: pytest.LogCaptureFixture
):
    mock_is_checkpoint_enabled.return_value = False
    caplog.set_level(level=logging.WARNING, logger=LOGGER_NAME)

    df = MagicMock()
    checkpoint_name = "test_checkpoint"
    result = validate_dataframe_checkpoint(df=df, checkpoint_name=checkpoint_name)

    mock_is_checkpoint_enabled.assert_called_once_with(checkpoint_name)
    assert result is None
    assert "disabled" in caplog.text
    assert checkpoint_name in caplog.text

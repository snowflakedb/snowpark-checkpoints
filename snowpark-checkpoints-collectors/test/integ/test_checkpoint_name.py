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

from pathlib import Path

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.singleton import Singleton


TELEMETRY_FOLDER = "telemetry"


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture(autouse=True)
def singleton():
    Singleton._instances = {}


@pytest.fixture(scope="function")
def output_path():
    folder = os.urandom(8).hex()
    directory = Path(tempfile.gettempdir()).resolve() / folder
    os.makedirs(directory)
    return str(directory)


def test_collect_dataframe_with_invalid_checkpoint_name(
    spark_session: SparkSession, output_path: str, caplog: pytest.LogCaptureFixture
):
    checkpoint_name = "6*invalid"
    pyspark_df = spark_session.createDataFrame(data=[], schema=StructType())
    expected_error_msg = (
        f"Invalid checkpoint name: {checkpoint_name}. "
        f"Checkpoint names must only contain alphanumeric characters, underscores and dollar signs."
    )

    with (
        pytest.raises(Exception) as ex_info,
        caplog.at_level(
            level=logging.ERROR,
            logger="snowflake.snowpark_checkpoints_collector.summary_stats_collector",
        ),
    ):
        collect_dataframe_checkpoint(
            pyspark_df, checkpoint_name=checkpoint_name, output_path=output_path
        )

    assert expected_error_msg == str(ex_info.value)
    assert expected_error_msg in caplog.text

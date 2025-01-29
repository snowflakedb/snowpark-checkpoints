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
import os
from pathlib import Path
import time

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, BooleanType

from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionResult,
)
from snowflake.snowpark_checkpoints_collector.collection_result.model.collection_point_result import (
    RESULT_KEY,
)
from snowflake.snowpark_checkpoints_collector.collection_result.model.collection_point_result_manager import (
    RESULTS_KEY,
)
from snowflake.snowpark_checkpoints_collector.utils import file_utils
from snowflake.snowpark_checkpoints_collector.singleton import Singleton
import tempfile


@pytest.fixture(scope="function")
def output_path():
    folder = os.urandom(8).hex()
    directory = str(Path(tempfile.gettempdir()).resolve() / folder)
    os.makedirs(directory)
    return directory


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def singleton():
    Singleton._instances = {}


def test_collect_dataframe_mode_schema(spark_session, singleton, output_path):

    execute_dataframe_collection(spark_session, CheckpointMode.SCHEMA, output_path)
    validate_collection_point_result_file(CollectionResult.PASS, output_path)


def test_collect_dataframe_mode_dataframe(spark_session, singleton, output_path):
    execute_dataframe_collection(spark_session, CheckpointMode.DATAFRAME, output_path)
    validate_collection_point_result_file(CollectionResult.PASS, output_path)


def test_collect_with_exception(spark_session, singleton, output_path):
    pyspark_df = spark_session.createDataFrame(
        [(1, 10), (2, 20), (3, 30), (4, 40)], schema="code integer, price integer"
    )
    with pytest.raises(Exception) as ex_info:
        collect_dataframe_checkpoint(
            pyspark_df,
            checkpoint_name="df_checkpoint_failed",
            mode=3,
            output_path=output_path,
        )
    assert "Invalid mode value." == str(ex_info.value)

    validate_collection_point_result_file(CollectionResult.FAIL, output_path)


def collect_dataframe_1(spark_session, checkpoint_name, mode, output_path):
    pyspark_df = spark_session.createDataFrame(
        [("Mario", "Bros"), ("Bruce", "Wayne"), ("Richard", "Grayson")],
        schema="name1 string, name2 string",
    )

    collect_dataframe_checkpoint(
        pyspark_df, checkpoint_name=checkpoint_name, mode=mode, output_path=output_path
    )


def collect_dataframe_2(spark_session, checkpoint_name, mode, output_path):
    pyspark_df = spark_session.createDataFrame(
        [(1, 10), (2, 20), (3, 30), (4, 40)], schema="code integer, price integer"
    )

    collect_dataframe_checkpoint(
        pyspark_df, checkpoint_name=checkpoint_name, mode=mode, output_path=output_path
    )


def collect_dataframe_3(spark_session, checkpoint_name, mode, output_path):
    data = []
    columns = StructType(
        [
            StructField("Code", LongType(), True),
            StructField("Active", BooleanType(), True),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    collect_dataframe_checkpoint(
        pyspark_df, checkpoint_name=checkpoint_name, mode=mode, output_path=output_path
    )


def execute_dataframe_collection(spark_session, mode, output_path):
    collect_dataframe_1(spark_session, "df_collect_1", mode, output_path)
    collect_dataframe_2(spark_session, "df_collect_2", mode, output_path)
    collect_dataframe_3(spark_session, "df_collect_3", mode, output_path)


def validate_collection_point_result_file(
    expected_result_value: CollectionResult, output_path
):
    result_file_path = file_utils.get_output_file_path(output_path)
    with open(result_file_path) as f:
        result_file_content = f.read()

    results_data_dict = json.loads(result_file_content)
    result_file_collection = results_data_dict[RESULTS_KEY]
    for result_file in result_file_collection:
        assert result_file[RESULT_KEY] == expected_result_value.value

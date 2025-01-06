#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, BooleanType

import integration_test_utils
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
from snowflake.snowpark_checkpoints_collector import Singleton


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def singleton():
    Singleton._instances = {}


def test_collect_dataframe_mode_schema(spark_session, singleton):
    execute_dataframe_collection(spark_session, CheckpointMode.SCHEMA)
    validate_collection_point_result_file(CollectionResult.PASS)


def test_collect_dataframe_mode_dataframe(spark_session, singleton):
    execute_dataframe_collection(spark_session, CheckpointMode.DATAFRAME)
    validate_collection_point_result_file(CollectionResult.PASS)


def test_collect_with_exception(spark_session, singleton):
    data = []
    columns = StructType()
    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    with pytest.raises(Exception) as ex_info:
        collect_dataframe_checkpoint(
            pyspark_df,
            checkpoint_name="df_checkpoint_failed",
            mode=CheckpointMode.SCHEMA,
        )
    assert "It is not possible to collect an empty DataFrame without schema" == str(
        ex_info.value
    )

    validate_collection_point_result_file(CollectionResult.FAIL)


def collect_dataframe_1(spark_session, checkpoint_name, mode):
    pyspark_df = spark_session.createDataFrame(
        [("Mario", "Bros"), ("Bruce", "Wayne"), ("Richard", "Grayson")],
        schema="name1 string, name2 string",
    )

    collect_dataframe_checkpoint(pyspark_df, checkpoint_name=checkpoint_name, mode=mode)


def collect_dataframe_2(spark_session, checkpoint_name, mode):
    pyspark_df = spark_session.createDataFrame(
        [(1, 10), (2, 20), (3, 30), (4, 40)], schema="code integer, price integer"
    )

    collect_dataframe_checkpoint(pyspark_df, checkpoint_name=checkpoint_name, mode=mode)


def collect_dataframe_3(spark_session, checkpoint_name, mode):
    data = []
    columns = StructType(
        [
            StructField("Code", LongType(), True),
            StructField("Active", BooleanType(), True),
        ]
    )

    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    collect_dataframe_checkpoint(pyspark_df, checkpoint_name=checkpoint_name, mode=mode)


def execute_dataframe_collection(spark_session, mode):
    collect_dataframe_1(spark_session, "df_collect_1", mode)
    collect_dataframe_2(spark_session, "df_collect_2", mode)
    collect_dataframe_3(spark_session, "df_collect_3", mode)


def validate_collection_point_result_file(expected_result_value: CollectionResult):
    result_file_path = file_utils.get_output_file_path()
    with open(result_file_path) as f:
        result_file_content = f.read()

    results_data_dict = json.loads(result_file_content)
    result_file_collection = results_data_dict[RESULTS_KEY]
    for result_file in result_file_collection:
        assert result_file[RESULT_KEY] == expected_result_value.value

    integration_test_utils.remove_output_directory()

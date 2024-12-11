#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import shutil

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, BooleanType

from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import (
    CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    CheckpointMode,
)
from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)

TEST_COLLECT_DF_EXPECTED_DIRECTORY_NAME = "test_collect_df_mode_2_expected"


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_collect_dataframe(spark_session):
    checkpoint_name = "test_full_df"
    checkpoint_file_name = CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )

    pyspark_df = spark_session.createDataFrame(
        [("Raul", 21), ("John", 34), ("Rose", 50)], schema="name string, age integer"
    )

    collect_dataframe_checkpoint(
        pyspark_df, checkpoint_name=checkpoint_name, mode=CheckpointMode.DATAFRAME
    )

    expected_schema = pyspark_df.schema

    validate_checkpoint_output(
        checkpoint_name, checkpoint_file_name, spark_session, expected_schema
    )


def test_collect_empty_dataframe_with_schema(spark_session):
    checkpoint_name = "test_empty_df_with_schema"
    checkpoint_file_name = CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )

    data = []
    columns = StructType(
        [
            StructField("Code", LongType(), True),
            StructField("Active", BooleanType(), True),
        ]
    )
    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    collect_dataframe_checkpoint(
        pyspark_df, checkpoint_name=checkpoint_name, mode=CheckpointMode.DATAFRAME
    )

    expected_schema = columns

    validate_checkpoint_output(
        checkpoint_name, checkpoint_file_name, spark_session, expected_schema
    )


def test_collect_empty_dataframe_without_schema(spark_session):
    data = []
    columns = StructType()
    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    with pytest.raises(Exception) as ex_info:
        collect_dataframe_checkpoint(
            pyspark_df, checkpoint_name="", mode=CheckpointMode.DATAFRAME
        )
    assert "It is not possible to collect an empty DataFrame without schema" == str(
        ex_info.value
    )


def validate_checkpoint_output(
    checkpoint_name, checkpoint_file_name, spark_session, expected_schema
) -> None:
    output_directory_path = get_output_directory_path(checkpoint_file_name)

    assert os.path.exists(output_directory_path) == True
    assert os.path.isdir(output_directory_path) == True

    output_directory_file_collection = os.listdir(output_directory_path)

    assert len(output_directory_file_collection) > 0

    output_df = spark_session.read.parquet(output_directory_path)
    compare_schema(output_df.schema, expected_schema)

    remove_output_directory()

    snow_connection = SnowConnection()
    df_table_data = snow_connection.session.read.table(checkpoint_name)
    assert len(df_table_data.schema.fields) > 0


def compare_schema(
    output_df_schema: StructType, expected_df_schema: StructType
) -> bool:
    output_df_schema_fields = output_df_schema.fields
    expected_df_schema_field = expected_df_schema.fields
    return output_df_schema_fields == expected_df_schema_field


def get_output_directory_path(checkpoint_file_name) -> str:
    current_directory_path = os.getcwd()
    output_file_path = os.path.join(
        current_directory_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        checkpoint_file_name,
    )
    return output_file_path


def remove_output_directory() -> None:
    current_directory_path = os.getcwd()
    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    if os.path.exists(output_directory_path):
        shutil.rmtree(output_directory_path)

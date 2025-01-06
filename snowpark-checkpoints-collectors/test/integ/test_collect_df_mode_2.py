#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from datetime import datetime
import os
import tempfile

from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as SparkDataFrame
from pyspark.sql.types import BooleanType, LongType, StructField, StructType
import pytest
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

from pyspark.sql.functions import col
from pyspark.sql.types import (
    DoubleType as SparkDoubleType,
    StringType as SparkStringType,
)
import integration_test_utils
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import (
    CheckpointMode,
)
from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)
from snowflake.snowpark_checkpoints_collector import Singleton

import pyspark.sql.types as t

from snowflake.snowpark_checkpoints_collector.summary_stats_collector import (
    generate_parquet_for_spark_df,
)


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()

@pytest.fixture
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


def test_collect_dataframe(spark_session, data, spark_schema, snowpark_schema):
    checkpoint_name = "test_full_df"

    pyspark_df = spark_session.createDataFrame(data, schema=spark_schema).orderBy(
        "INTEGER"
    )

    temp_dir = tempfile.gettempdir()
    output_path = os.path.join(
        temp_dir, f"test_collect_dataframe_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )

    collect_dataframe_checkpoint(
        pyspark_df,
        checkpoint_name=checkpoint_name,
        mode=CheckpointMode.DATAFRAME,
        output_path=output_path,
    )

    validate_dataframes(checkpoint_name, pyspark_df, snowpark_schema)


def test_collect_empty_dataframe_with_schema(
    spark_session, spark_schema, snowpark_schema
):
    checkpoint_name = "test_empty_df_with_schema"

    data = []
    pyspark_df = spark_session.createDataFrame(data=data, schema=spark_schema)
    expected_df = pyspark_df.toPandas()
    expected_df.columns = expected_df.columns.str.upper()

    collect_dataframe_checkpoint(
        pyspark_df, checkpoint_name=checkpoint_name, mode=CheckpointMode.DATAFRAME
    )

    validate_dataframes(checkpoint_name, pyspark_df, snowpark_schema)


def test_collect_invalid_mode(spark_session, data, spark_schema):
    pyspark_df = spark_session.createDataFrame(data=data, schema=spark_schema)

    with pytest.raises(Exception) as ex_info:
        collect_dataframe_checkpoint(pyspark_df, checkpoint_name="", mode=3)
    assert "Invalid mode value." == str(ex_info.value)


def test_spark_df_mode_dataframe(spark_schema, snowpark_schema, data):
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(data, schema=spark_schema)
    parquet_directory = os.path.join(
        tempfile.gettempdir(),
        f"test_spark_df_checkpoint_{datetime.now().strftime('%Y%m%d%H%M%S')}",
    )

    generate_parquet_for_spark_df(spark_df, parquet_directory)

    snow = SnowConnection()
    snow.create_snowflake_table_from_local_parquet(
        "test_spark_df_checkpoint", parquet_directory
    )

    validate_dataframes("test_spark_df_checkpoint", spark_df, snowpark_schema)


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

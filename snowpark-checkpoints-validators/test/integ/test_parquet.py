from datetime import datetime
from pytest import fixture
from snowflake.snowpark import Session
from snowflake.snowpark_checkpoints.checkpoint import validate_dataframe_checkpoint
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from pyspark.sql import SparkSession

from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    ByteType,
    ShortType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
)
from snowflake.snowpark_checkpoints.utils.constant import CheckpointMode


@fixture
def job_context():
    session = Session.builder.getOrCreate()
    job_context = SnowparkJobContext(
        session, SparkSession.builder.getOrCreate(), "realdemo", True
    )
    return job_context


@fixture
def schema():
    return StructType(
        [
            StructField("byte", ByteType(), True),
            StructField("short", ShortType(), True),
            StructField("interger", IntegerType(), True),
            StructField("long", LongType(), True),
            # StructField("float", FloatType(), True),
            StructField("double", DoubleType(), True),
            # StructField("decimal", DecimalType(10, 3), True),
            StructField("string", StringType(), True),
            # StructField("binary", BinaryType(), True),
            StructField("boolean", BooleanType(), True),
            StructField("date", DateType(), True),
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
            # 7.8,
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
            # 0.12,
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
            # 3.45,
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
            # 6.78,
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
            # 9.01,
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
            # 1.23,
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
            # 4.56,
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
            # 7.8,
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
            # 0.12,
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
            # 3.45,
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


def test_df_mode_dataframe(job_context, schema, data):
    checkpoint_name = "test_mode_dataframe_checkpoint"
    df = job_context.snowpark_session.create_dataframe(data, schema)
    df.write.save_as_table(checkpoint_name, mode="overwrite")

    try:
        validate_dataframe_checkpoint(
            df,
            checkpoint_name,
            job_context=job_context,
            mode=CheckpointMode.DATAFRAME,
        )
    except:
        assert False, "Should not raise any exception"


def test_df_mode_dataframe_fail(job_context, schema, data):
    checkpoint_name = "test_mode_dataframe_checkpoint_fail"
    data.pop()
    df = job_context.snowpark_session.create_dataframe(data, schema)
    df.write.save_as_table(checkpoint_name, mode="overwrite")

    try:
        validate_dataframe_checkpoint(
            df,
            checkpoint_name,
            job_context=job_context,
            mode=CheckpointMode.DATAFRAME,
        )
        assert False, "Should raise an exception"
    except:
        pass

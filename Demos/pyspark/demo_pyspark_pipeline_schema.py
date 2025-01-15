from decimal import Decimal
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    ByteType,
    ShortType,
    StructField,
    LongType,
    IntegerType,
    FloatType,
    StringType,
    DoubleType,
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
    TimestampNTZType,
    DecimalType,
)
from datetime import datetime

from snowflake.snowpark_checkpoints_collector.collection_common import (
    SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR,
)

date_format = "%Y-%m-%d"
timestamp_format = "%Y-%m-%d %H:%M:%S"
timestamp_ntz_format = "%Y-%m-%d %H:%M:%S"

os.environ[SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR] = "Demos/pyspark"

spark = SparkSession.builder.appName("demo").getOrCreate()

schema = StructType(
    [
        # Numeric Types
        # ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
        StructField("byte", ByteType(), True),
        # ShortType: Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
        StructField("short", ShortType(), True),
        # IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
        StructField("interger", IntegerType(), True),
        # LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
        StructField("long", LongType(), True),
        # ! CORNER CASE
        # FloatType: Represents 4-byte single-precision floating point numbers.
        StructField("float", FloatType(), True),
        # DoubleType: Represents 8-byte double-precision floating point numbers.
        StructField("double", DoubleType(), True),
        # ! CORNER CASE
        # DecimalType: Represents arbitrary-precision signed decimal numbers. Backed internally by
        StructField("decimal", DecimalType(10, 3), True),
        # StringType
        # StringType: Represents character string values.
        StructField("string", StringType(), True),
        # BinaryType
        # ! CORNER CASE
        # BinaryType: Represents byte sequence values.
        StructField("binary", BinaryType(), True),
        # Boolean type
        # BooleanType: Represents boolean values.
        StructField("boolean", BooleanType(), True),
        # Datetime type
        # DateType: Represents values comprising values of fields year, month and day, without a time-zone.
        StructField("date", DateType(), True),
        # ! CORNER CASE
        # TimestampType: Timestamp with local time zone(TIMESTAMP_LTZ). It represents values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. The timestamp value represents an absolute point in time.
        StructField("timestamp", TimestampType(), True),
        # ! CORNER CASE
        # TimestampNTZType: Timestamp without time zone(TIMESTAMP_NTZ). It represents values comprising values of fields year, month, day, hour, minute, and second. All operations are performed without taking any time zone into account.
        # Note: TIMESTAMP in Spark is a user-specified alias associated with one of the TIMESTAMP_LTZ and TIMESTAMP_NTZ variations. Users can set the default timestamp type as TIMESTAMP_LTZ(default value) or TIMESTAMP_NTZ via the configuration spark.sql.timestampType.
        StructField("timestamp_ntz", TimestampNTZType(), True),
    ]
)

data = [
    Row(
        byte=3,
        short=789,
        interger=13579,
        long=1231231231,
        float=7.8,
        double=2.345678,
        decimal=Decimal(7.891),
        string="red",
        binary=b"info",
        boolean=True,
        date=datetime.strptime("2023-03-01", date_format),
        timestamp=datetime.strptime("2023-03-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-03-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=4,
        short=101,
        interger=24680,
        long=3213213210,
        float=0.12,
        double=3.456789,
        decimal=Decimal(0.123),
        string="red",
        binary=b"test",
        boolean=False,
        date=datetime.strptime("2023-04-01", date_format),
        timestamp=datetime.strptime("2023-04-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-04-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=5,
        short=202,
        interger=36912,
        long=4564564560,
        float=3.45,
        double=4.567890,
        decimal=Decimal(3.456),
        string="red",
        binary=b"example2",
        boolean=True,
        date=datetime.strptime("2023-05-01", date_format),
        timestamp=datetime.strptime("2023-05-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-05-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=6,
        short=303,
        interger=48123,
        long=7897897890,
        float=6.78,
        double=5.678901,
        decimal=Decimal(6.789),
        string="red",
        binary=b"sample2",
        boolean=False,
        date=datetime.strptime("2023-06-01", date_format),
        timestamp=datetime.strptime("2023-06-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-06-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=7,
        short=404,
        interger=59234,
        long=9879879870,
        float=9.01,
        double=6.789012,
        decimal=Decimal(9.012),
        string="red",
        binary=b"data2",
        boolean=True,
        date=datetime.strptime("2023-07-01", date_format),
        timestamp=datetime.strptime("2023-07-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-07-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=8,
        short=505,
        interger=70345,
        long=1231231234,
        float=1.23,
        double=7.890123,
        decimal=Decimal(1.234),
        string="blue",
        binary=b"test2",
        boolean=False,
        date=datetime.strptime("2023-08-01", date_format),
        timestamp=datetime.strptime("2023-08-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-08-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=9,
        short=606,
        interger=81456,
        long=3213213214,
        float=4.56,
        double=8.901234,
        decimal=Decimal(4.567),
        string="blue",
        binary=b"example3",
        boolean=True,
        date=datetime.strptime("2023-09-01", date_format),
        timestamp=datetime.strptime("2023-09-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-09-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=10,
        short=707,
        interger=92567,
        long=4564564564,
        float=7.8,
        double=9.012345,
        decimal=Decimal(7.892),
        string="blue",
        binary=b"sample3",
        boolean=False,
        date=datetime.strptime("2023-10-01", date_format),
        timestamp=datetime.strptime("2023-10-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-10-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=11,
        short=808,
        interger=103678,
        long=7897897894,
        float=0.12,
        double=0.123456,
        decimal=Decimal(0.123),
        string="green",
        binary=b"data3",
        boolean=True,
        date=datetime.strptime("2023-11-01", date_format),
        timestamp=datetime.strptime("2023-11-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-11-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=12,
        short=909,
        interger=114789,
        long=9879879874,
        float=3.45,
        double=1.234567,
        decimal=Decimal(3.456),
        string="green",
        binary=b"test3",
        boolean=False,
        date=datetime.strptime("2023-12-01", date_format),
        timestamp=datetime.strptime("2023-12-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-12-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=1,
        short=123,
        interger=12345,
        long=1234567890,
        float=1.23,
        double=1.234567,
        decimal=Decimal(1.234),
        string="green",
        binary=b"binary",
        boolean=True,
        date=datetime.strptime("2023-01-01", date_format),
        timestamp=datetime.strptime("2023-01-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-01-01 12:00:00", timestamp_ntz_format),
    ),
    Row(
        byte=2,
        short=456,
        interger=67890,
        long=9876543210,
        float=4.56,
        double=7.890123,
        decimal=Decimal(4.567),
        string="blue",
        binary=b"data",
        boolean=False,
        date=datetime.strptime("2023-02-01", date_format),
        timestamp=datetime.strptime("2023-02-01 12:00:00", timestamp_format),
        timestamp_ntz=datetime.strptime("2023-02-01 12:00:00", timestamp_ntz_format),
    ),
]

df = spark.createDataFrame(data, schema)

# Collect a schema/stats here!
collect_dataframe_checkpoint(
    df, "demo_initial_creation_checkpoint", sample=0.5, output_path="Demos/snowpark"
)

df1 = df.withColumn(
    "life_stage",
    when(col("byte") < 4, "child")
    .when(col("byte").between(4, 10), "teenager")
    .otherwise("adult"),
)

# Collect a schema/stats here!
collect_dataframe_checkpoint(
    df1, "demo_add_a_column", sample=0.5, output_path="Demos/snowpark"
)

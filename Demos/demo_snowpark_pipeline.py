from datetime import datetime
from snowflake.snowpark import Session

from pyspark.sql import SparkSession
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    ByteType,
    ShortType,
    LongType,
    FloatType,
    DoubleType,
    BinaryType,
    BooleanType,
    DateType,
    TimestampType,
)
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.checkpoint import check_df_schema_file
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark

session = Session.builder.getOrCreate()
job_context = SnowparkJobContext(
    session, SparkSession.builder.getOrCreate(), "realdemo", True
)

date_format = "%Y-%m-%d"
timestamp_format = "%Y-%m-%d %H:%M:%S"
timestamp_ntz_format = "%Y-%m-%d %H:%M:%S"

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
        # FloatType: Represents 4-byte single-precision floating point numbers.
        StructField("float", FloatType(), True),
        # DoubleType: Represents 8-byte double-precision floating point numbers.
        StructField("double", DoubleType(), True),
        #
        # ! CORNER CASE
        # DecimalType: Represents arbitrary-precision signed decimal numbers. Backed internally by
        # StructField("decimal", DecimalType(10, 3), True),
        # StringType
        # StringType: Represents character string values.
        StructField("string", StringType(), True),
        # BinaryType
        # BinaryType: Represents byte sequence values.
        StructField("binary", BinaryType(), True),
        # Boolean type
        # BooleanType: Represents boolean values.
        StructField("boolean", BooleanType(), True),
        # Datetime type
        # DateType: Represents values comprising values of fields year, month and day, without a time-zone.
        StructField("date", DateType(), True),
        # TimestampType: Timestamp with local time zone(TIMESTAMP_LTZ). It represents values comprising values of fields year, month, day, hour, minute, and second, with the session local time-zone. The timestamp value represents an absolute point in time.
        StructField("timestamp", TimestampType(), True),
        # TimestampNTZType: Timestamp without time zone(TIMESTAMP_NTZ). It represents values comprising values of fields year, month, day, hour, minute, and second. All operations are performed without taking any time zone into account.
        # Note: TIMESTAMP in Spark is a user-specified alias associated with one of the TIMESTAMP_LTZ and TIMESTAMP_NTZ variations. Users can set the default timestamp type as TIMESTAMP_LTZ(default value) or TIMESTAMP_NTZ via the configuration spark.sql.timestampType.
        StructField("timestamp_ntz", TimestampType(), True),
    ]
)

data = [
    [
        3,
        789,
        13579,
        1231231231,
        7.8,
        2.345678,
        # Decimal(7.891),
        "red",
        b"info",
        True,
        datetime.strptime("2023-03-01", date_format),
        datetime.strptime("2023-03-01 12:00:00", timestamp_format),
        datetime.strptime("2023-03-01 12:00:00", timestamp_ntz_format),
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
        b"test",
        False,
        datetime.strptime("2023-04-01", date_format),
        datetime.strptime("2023-04-01 12:00:00", timestamp_format),
        datetime.strptime("2023-04-01 12:00:00", timestamp_ntz_format),
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
        b"example2",
        True,
        datetime.strptime("2023-05-01", date_format),
        datetime.strptime("2023-05-01 12:00:00", timestamp_format),
        datetime.strptime("2023-05-01 12:00:00", timestamp_ntz_format),
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
        b"sample2",
        False,
        datetime.strptime("2023-06-01", date_format),
        datetime.strptime("2023-06-01 12:00:00", timestamp_format),
        datetime.strptime("2023-06-01 12:00:00", timestamp_ntz_format),
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
        b"data2",
        True,
        datetime.strptime("2023-07-01", date_format),
        datetime.strptime("2023-07-01 12:00:00", timestamp_format),
        datetime.strptime("2023-07-01 12:00:00", timestamp_ntz_format),
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
        b"test2",
        False,
        datetime.strptime("2023-08-01", date_format),
        datetime.strptime("2023-08-01 12:00:00", timestamp_format),
        datetime.strptime("2023-08-01 12:00:00", timestamp_ntz_format),
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
        b"example3",
        True,
        datetime.strptime("2023-09-01", date_format),
        datetime.strptime("2023-09-01 12:00:00", timestamp_format),
        datetime.strptime("2023-09-01 12:00:00", timestamp_ntz_format),
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
        b"sample3",
        False,
        datetime.strptime("2023-10-01", date_format),
        datetime.strptime("2023-10-01 12:00:00", timestamp_format),
        datetime.strptime("2023-10-01 12:00:00", timestamp_ntz_format),
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
        b"data3",
        True,
        datetime.strptime("2023-11-01", date_format),
        datetime.strptime("2023-11-01 12:00:00", timestamp_format),
        datetime.strptime("2023-11-01 12:00:00", timestamp_ntz_format),
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
        b"test3",
        False,
        datetime.strptime("2023-12-01", date_format),
        datetime.strptime("2023-12-01 12:00:00", timestamp_format),
        datetime.strptime("2023-12-01 12:00:00", timestamp_ntz_format),
    ],
    [
        1,
        123,
        12345,
        1234567890,
        1.23,
        1.234567,
        # Decimal(1.234),
        "green",
        b"binary",
        True,
        datetime.strptime("2023-01-01", date_format),
        datetime.strptime("2023-01-01 12:00:00", timestamp_format),
        datetime.strptime("2023-01-01 12:00:00", timestamp_ntz_format),
    ],
    [
        2,
        456,
        67890,
        9876543210,
        4.56,
        7.890123,
        # Decimal(4.567),
        "blue",
        b"data",
        False,
        datetime.strptime("2023-02-01", date_format),
        datetime.strptime("2023-02-01 12:00:00", timestamp_format),
        datetime.strptime("2023-02-01 12:00:00", timestamp_ntz_format),
    ],
]

df = session.create_dataframe(data, schema)

# Check a schema/stats here!
check_df_schema_file(df, "demo-initial-creation-checkpoint", job_context)


def original_spark_code_I_dont_understand(df):
    from pyspark.sql.functions import col, when

    ret = df.withColumn(
        "life_stage",
        when(col("byte") < 4, "child")
        .when(col("byte").between(4, 10), "teenager")
        .otherwise("adult"),
    )
    return ret


@check_with_spark(
    job_context=job_context, spark_function=original_spark_code_I_dont_understand
)
def new_snowpark_code_I_do_understand(df):
    from snowflake.snowpark.functions import col, lit, when

    ref = df.with_column(
        "life_stage",
        when(col("byte") < 4, lit("child"))
        .when(col("byte").between(4, 10), lit("teenager"))
        .otherwise(lit("adult")),
    )
    return ref


df1 = new_snowpark_code_I_do_understand(df)

# Check a schema/stats here!
check_df_schema_file(df1, "demo-add-a-column", job_context)

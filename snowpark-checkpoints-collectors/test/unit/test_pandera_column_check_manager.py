import datetime

import pandera
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
    DayTimeIntervalType,
    IntegerType,
    StringType,
    TimestampType,
    TimestampNTZType,
)

from snowflake.snowpark_checkpoints_collector.collection_common import (
    BOOLEAN_COLUMN_TYPE,
    DAYTIMEINTERVAL_COLUMN_TYPE,
    INTEGER_COLUMN_TYPE,
    STRING_COLUMN_TYPE,
    TIMESTAMP_COLUMN_TYPE,
    TIMESTAMP_NTZ_COLUMN_TYPE,
)
from snowflake.snowpark_checkpoints_collector.column_pandera_checks import (
    PanderaColumnChecksManager,
)

ISIN_CHECK_NAME = "isin"
IN_RANGE_CHECK_NAME = "in_range"
GREATER_THAN_OR_EQUAL_TO_CHECK_NAME = "greater_than_or_equal_to"
LESS_THAN_OR_EQUAL_TO_CHECK_NAME = "less_than_or_equal_to"
STR_LENGTH_CHECK_NAME = "str_length"


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_add_boolean_type_checks(spark_session):
    clm_name = "clmTest"
    clm_type = BOOLEAN_COLUMN_TYPE

    data = [[True], [False], [False], [False], [None]]
    struct_field = StructField(clm_name, BooleanType(), True)
    schema = StructType([struct_field])
    pyspark_df = spark_session.createDataFrame(data, schema)

    pandas_df = pyspark_df.toPandas()
    DataFrameSchema = pandera.infer_schema(pandas_df)
    pandera_column = DataFrameSchema.columns[clm_name]

    manager = PanderaColumnChecksManager()
    manager.add_checks_column(clm_name, clm_type, pyspark_df, pandera_column)

    assert len(pandera_column.checks) == 1
    assert pandera_column.checks[0].name == ISIN_CHECK_NAME


def test_add_daytimeinterval_type_checks(spark_session):
    clm_name = "clmTest"
    clm_type = DAYTIMEINTERVAL_COLUMN_TYPE

    data = [
        [datetime.timedelta(13)],
        [datetime.timedelta(14)],
        [datetime.timedelta(15)],
        [datetime.timedelta(20)],
        [None],
    ]
    struct_field = StructField(clm_name, DayTimeIntervalType(), True)
    schema = StructType([struct_field])
    pyspark_df = spark_session.createDataFrame(data, schema)

    pandas_df = pyspark_df.toPandas()
    DataFrameSchema = pandera.infer_schema(pandas_df)
    pandera_column = DataFrameSchema.columns[clm_name]

    manager = PanderaColumnChecksManager()
    manager.add_checks_column(clm_name, clm_type, pyspark_df, pandera_column)

    assert len(pandera_column.checks) == 1
    assert pandera_column.checks[0].name == IN_RANGE_CHECK_NAME


def test_add_numeric_type_checks(spark_session):
    clm_name = "clmTest"
    clm_type = INTEGER_COLUMN_TYPE

    data = [
        [41034250],
        [120],
        [54012],
        [90],
        [None],
    ]
    struct_field = StructField(clm_name, IntegerType(), True)
    schema = StructType([struct_field])
    pyspark_df = spark_session.createDataFrame(data, schema)

    pandas_df = pyspark_df.toPandas()
    DataFrameSchema = pandera.infer_schema(pandas_df)
    pandera_column = DataFrameSchema.columns[clm_name]

    manager = PanderaColumnChecksManager()
    manager.add_checks_column(clm_name, clm_type, pyspark_df, pandera_column)

    assert len(pandera_column.checks) == 3
    assert pandera_column.checks[0].name == GREATER_THAN_OR_EQUAL_TO_CHECK_NAME
    assert pandera_column.checks[1].name == LESS_THAN_OR_EQUAL_TO_CHECK_NAME
    assert pandera_column.checks[2].name == IN_RANGE_CHECK_NAME


def test_add_string_type_checks(spark_session):
    clm_name = "clmTest"
    clm_type = STRING_COLUMN_TYPE

    data = [
        ["A1"],
        [""],
        ["ABCDEF"],
        ["hy"],
        [None],
    ]
    struct_field = StructField(clm_name, StringType(), True)
    schema = StructType([struct_field])
    pyspark_df = spark_session.createDataFrame(data, schema)

    pandas_df = pyspark_df.toPandas()
    DataFrameSchema = pandera.infer_schema(pandas_df)
    pandera_column = DataFrameSchema.columns[clm_name]

    manager = PanderaColumnChecksManager()
    manager.add_checks_column(clm_name, clm_type, pyspark_df, pandera_column)

    assert len(pandera_column.checks) == 1
    assert pandera_column.checks[0].name == STR_LENGTH_CHECK_NAME


def test_add_timestamp_type_checks(spark_session):
    clm_name = "clmTest"
    clm_type = TIMESTAMP_COLUMN_TYPE

    data = [
        [datetime.datetime(2000, 10, 10, 10, 10, 10)],
        [datetime.datetime(1998, 5, 5, 5, 5, 5)],
        [datetime.datetime(2012, 3, 7, 2, 0, 10)],
        [datetime.datetime(2025, 12, 20, 0, 0, 0)],
        [None],
    ]
    struct_field = StructField(clm_name, TimestampType(), True)
    schema = StructType([struct_field])
    pyspark_df = spark_session.createDataFrame(data, schema)

    pandas_df = pyspark_df.toPandas()
    DataFrameSchema = pandera.infer_schema(pandas_df)
    pandera_column = DataFrameSchema.columns[clm_name]

    manager = PanderaColumnChecksManager()
    manager.add_checks_column(clm_name, clm_type, pyspark_df, pandera_column)

    assert len(pandera_column.checks) == 3
    assert pandera_column.checks[0].name == GREATER_THAN_OR_EQUAL_TO_CHECK_NAME
    assert pandera_column.checks[1].name == LESS_THAN_OR_EQUAL_TO_CHECK_NAME
    assert pandera_column.checks[2].name == IN_RANGE_CHECK_NAME


def test_add_timestamp_ntz_type_checks(spark_session):
    clm_name = "clmTest"
    clm_type = TIMESTAMP_NTZ_COLUMN_TYPE

    data = [
        [datetime.datetime(2000, 10, 10, 10, 10, 10)],
        [datetime.datetime(1998, 5, 5, 5, 5, 5)],
        [datetime.datetime(2012, 3, 7, 2, 0, 10)],
        [datetime.datetime(2025, 12, 20, 0, 0, 0)],
        [None],
    ]
    struct_field = StructField(clm_name, TimestampNTZType(), True)
    schema = StructType([struct_field])
    pyspark_df = spark_session.createDataFrame(data, schema)

    pandas_df = pyspark_df.toPandas()
    DataFrameSchema = pandera.infer_schema(pandas_df)
    pandera_column = DataFrameSchema.columns[clm_name]

    manager = PanderaColumnChecksManager()
    manager.add_checks_column(clm_name, clm_type, pyspark_df, pandera_column)

    assert len(pandera_column.checks) == 3
    assert pandera_column.checks[0].name == GREATER_THAN_OR_EQUAL_TO_CHECK_NAME
    assert pandera_column.checks[1].name == LESS_THAN_OR_EQUAL_TO_CHECK_NAME
    assert pandera_column.checks[2].name == IN_RANGE_CHECK_NAME

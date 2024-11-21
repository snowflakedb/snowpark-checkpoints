#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from snowflake.snowpark_checkpoints.checkpoint import check_output_with
from snowflake.snowpark import Session

from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession


session = Session.builder.getOrCreate()
spark_session = SparkSession.builder.getOrCreate()
job_context = SnowparkJobContext(session, spark_session, "demo-e2e-meaninful", True)


def my_spark_fn(df: SparkDataFrame):
    return df.filter(df.A < 2.0)


out_schema = DataFrameSchema(
    {
        "A": Column(float, Check(lambda x: 3 <= x <= 10, element_wise=True)),
        "B": Column(float, Check(lambda x: x < 5)),
    }
)


@check_with_spark(
    job_context=job_context,
    spark_function=my_spark_fn,
    sample=100,
    sampling_strategy=SamplingStrategy.RANDOM_SAMPLE,
)
@check_output_with(
    out_schema,
    sample=100,
    sampling_strategy=SamplingStrategy.RANDOM_SAMPLE,
    job_context=job_context,
)
def my_snowpark_fn(df: SnowparkDataFrame):
    return df.filter(df.a > 2.0)


df = job_context.snowpark_session.create_dataframe(
    [[1.1, 2.2], [3.3, 4.4]], schema=["a", "b"]
)

my_snowpark_fn(df)

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from snowflake.snowpark_checkpoints.checkpoint import check_output_schema
from snowflake.snowpark import Session

from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.spark_migration import (
    auto_migrate,
)
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

session = Session.builder.getOrCreate()
spark_session = SparkSession.builder.getOrCreate()
job_context = SnowparkJobContext(session, spark_session, "demo-e2e-auto-migrate", False)

df = job_context.snowpark_session.create_dataframe(
    [[1.1, 2.2], [3.3, 4.4]], schema=["a", "b"]
)

# Will take the annotated function and return a
# generated snowpark function; which is pickled and
# saved to disk after generation
@auto_migrate(job_context=job_context)
def my_spark_fn_2(df):
    return df.filter(df.A < 2.0)


# Calls to the spark function switch out the spark
# code with generated snowpark code
result = my_spark_fn_2(df)
print("Done!\n", result.to_pandas())

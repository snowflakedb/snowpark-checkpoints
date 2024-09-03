
from numpy import int8
import pandas as pd
from pandera import DataFrameSchema, Column, Check
import pandera
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from snowflake.snowpark_checkpoints.checkpoint import check_input_with_pandera, check_output_with_pandera
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit 

from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from pyspark.sql import DataFrame as SparkDataFrame
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark import Session
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np


session = Session.builder.getOrCreate()
spark_session = SparkSession.builder.getOrCreate()
job_context = SnowparkJobContext(session, spark_session, "demo-e2e-meaninful", True)


def my_spark_fn(df:SparkDataFrame):
    return df.filter(df.A < 2.0)

out_schema = DataFrameSchema({
    "A": Column(float,
         Check(lambda x: 3 <= x <= 10, element_wise=True)),
    "B": Column(float, Check(lambda x: x < 5)),
})

@check_with_spark(job_context=job_context, 
                spark_function=my_spark_fn,
                sample=100, 
                sampling_strategy=SamplingStrategy.RANDOM_SAMPLE, )
@check_output_with_pandera(out_schema,
                           sample=100, 
                           sampling_strategy=SamplingStrategy.RANDOM_SAMPLE, 
                           job_context=job_context)
def my_snowpark_fn(df:SnowparkDataFrame):
    return df.filter(df.a > 2.0)
    
df = job_context.snowpark_session.create_dataframe([[1.1, 2.2], [3.3, 4.4]], schema=["a", "b"])

my_snowpark_fn(df)


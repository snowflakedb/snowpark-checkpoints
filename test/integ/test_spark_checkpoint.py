
import snowflake
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark
from pyspark.sql import DataFrame as SparkDataFrame
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark import Session

def test_spark_checkpoint_scalar_passing():
    session = Session.builder.getOrCreate()

    job_context = SnowparkJobContext(session)

    def my_spark_fn(df:SparkDataFrame):
        return df.count()

    @check_with_spark(job_context=job_context, 
                      spark_function=my_spark_fn)
    def my_snowpark_fn(df:SnowparkDataFrame):
        return df.count()
    
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    count = my_snowpark_fn(df)
    assert(count == 2)

def test_spark_checkpoint_scalar_fail():
    session = Session.builder.getOrCreate()

    job_context = SnowparkJobContext(session)

    def my_spark_fn(df:SparkDataFrame):
        return df.count() + 1

    @check_with_spark(job_context=job_context, 
                      spark_function=my_spark_fn)
    def my_snowpark_fn(df:SnowparkDataFrame):
        return df.count()
    
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    try:
        my_snowpark_fn(df)
        assert(False, "Should have failed")
    except:
        pass


def test_spark_checkpoint_df_pass():
    session = Session.builder.getOrCreate()

    job_context = SnowparkJobContext(session)

    def my_spark_fn(df:SparkDataFrame):
        return df

    @check_with_spark(job_context=job_context, 
                      spark_function=my_spark_fn)
    def my_snowpark_fn(df:SnowparkDataFrame):
        return df
    
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    my_snowpark_fn(df)


def test_spark_checkpoint_df_fail():
    session = Session.builder.getOrCreate()

    job_context = SnowparkJobContext(session)

    def my_spark_fn(df:SparkDataFrame):
        return df.filter(df.A > 2)

    @check_with_spark(job_context=job_context, 
                      spark_function=my_spark_fn)
    def my_snowpark_fn(df:SnowparkDataFrame):
        return df.filter(df.a < 2)
    
    df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
    try:
        my_snowpark_fn(df)
        assert(False, "Should have failed")
    except:
        pass
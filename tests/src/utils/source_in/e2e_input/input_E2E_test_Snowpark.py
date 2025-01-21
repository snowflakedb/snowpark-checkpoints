from datetime import datetime
import pandas as pd
from snowflake.snowpark import Session
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints.utils.constants import CheckpointMode
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.checkpoint import validate_dataframe_checkpoint
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark

columns_names = ['INTEGER_TYPE', 'STRING_TYPE', 'SHORT_TYPE', 'LONG_TYPE', 'FLOAT_TYPE', 'DOUBLE_TYPE', 'BOOLEAN_TYPE', 'DATE_TYPE', 'TIMESTAMP_TYPE']
CHECKPOINT_NAME = "test_E2E_initial_checkpoint"

def input_E2E_test_Snowpark(execution_mode, temp_path):
    session = Session.builder.getOrCreate()
    job_context = SnowparkJobContext(
        session, SparkSession.builder.getOrCreate(), "E2E_Test", True
    )
    data = pd.read_csv("tests/src/utils/source_in/e2e_input/dataE2Etest_datatypes.csv",header=None, names=columns_names)
    
    df = session.create_dataframe(data, columns_names)
    checkpoint_mode = CheckpointMode.DATAFRAME if str(execution_mode).casefold() == "dataframe" else CheckpointMode.SCHEMA

    validate_dataframe_checkpoint(df, CHECKPOINT_NAME, job_context, mode=checkpoint_mode, output_path=temp_path)


    def original_spark_code(df):
        from pyspark.sql.functions import col, when

        ret = df.withColumn(
            'RESULT',
            when(col('INTEGER_TYPE') < 0, "Negative")
            .otherwise("Positive"),
        )
  
        return ret

    @check_with_spark(
        job_context=job_context, 
        spark_function=original_spark_code,
        checkpoint_name="snowpark_function",
        output_path=temp_path,
    )
    
    def new_snowpark_code(df):
        from snowflake.snowpark.functions import col, lit, when

        ref = df.with_column(
            'RESULT',
            when(col('"INTEGER_TYPE"') < 0, lit("Negative"))
            .otherwise(lit("Positive")),
        )
        return ref

    new_snowpark_code(df)
    return CHECKPOINT_NAME
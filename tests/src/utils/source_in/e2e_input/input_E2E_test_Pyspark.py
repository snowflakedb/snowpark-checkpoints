from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
import pandas as pd

def input_E2E_test_Pyspark(execution_mode, temp_path):

    columns_names = ['INTEGER_TYPE', 'STRING_TYPE', 'SHORT_TYPE', 'LONG_TYPE', 'FLOAT_TYPE', 'DOUBLE_TYPE', 'BOOLEAN_TYPE', 'DATE_TYPE', 'TIMESTAMP_TYPE']
    spark = SparkSession.builder.appName("E2E_Test").getOrCreate()

    data = pd.read_csv("tests/src/utils/source_in/e2e_input/dataE2Etest_datatypes.csv",header=None, names=columns_names)
    df = spark.createDataFrame(data)
    checkpoint_mode = CheckpointMode.DATAFRAME if str(execution_mode).casefold() == "dataframe" else CheckpointMode.SCHEMA

    collect_dataframe_checkpoint(df, "test_E2E_initial_checkpoint" ,mode=checkpoint_mode, output_path=temp_path)
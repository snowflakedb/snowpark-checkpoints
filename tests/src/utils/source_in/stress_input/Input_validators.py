import time
import pandas as pd
import psutil
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints.checkpoint import validate_dataframe_checkpoint
from snowflake.snowpark_checkpoints.utils.constants import CheckpointMode
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark import Session


def Input_validators(execution_mode, temp_path):

    session = Session.builder.getOrCreate()
    job_context = SnowparkJobContext(
        session, SparkSession.builder.getOrCreate(), "stress_tests", True
    )
    
    df = pd.read_csv("tests/src/utils/source_in/stress_input/data_input_medium.csv")
    dfSnowpark = session.create_dataframe(df)
    checkpoint_mode = CheckpointMode.DATAFRAME if str(execution_mode).casefold() == "dataframe" else CheckpointMode.SCHEMA
    start_time = time.time()
    validate_dataframe_checkpoint(dfSnowpark, "test_input_collectors_initial_checkpoint", job_context, mode=checkpoint_mode, output_path=temp_path)
    
    process = psutil.Process()
    memory = process.memory_info().rss / 1024 / 1024
    final_time = time.time() - start_time
    print("Memory: {} Time: {}".format(memory, final_time))
    session.close()
    return [round(memory,2), round(final_time,2)]

if __name__ == "__main__":
    Input_validators()
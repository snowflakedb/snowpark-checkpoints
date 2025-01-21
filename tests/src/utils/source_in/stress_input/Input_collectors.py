import time
import psutil
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
from snowflake.snowpark_checkpoints_collector.singleton import Singleton


def Input_collectors(execution_mode, temp_path):
    
    spark = SparkSession.builder.appName("stress_tests").getOrCreate()
    df = spark.read.csv("tests/src/utils/source_in/stress_input/data_input_medium.csv", header=True, inferSchema=True)
    checkpoint_mode = CheckpointMode.DATAFRAME if str(execution_mode).casefold() == "dataframe" else CheckpointMode.SCHEMA
    
    start_time = time.time()
    sample = 1.0 if execution_mode.casefold() == "dataframe" else 0.3
    collect_dataframe_checkpoint(df, "test_input_collectors_initial_checkpoint", sample=sample, mode=checkpoint_mode, output_path=temp_path) 
    
    process = psutil.Process()
    memory = process.memory_info().rss / 1024 / 1024
    final_time = time.time() - start_time
    print("Memory: {} Time: {}".format(memory, final_time))
    spark.stop()
    return [round(memory,2), round(final_time,2)]

if __name__ == "__main__":
    Input_collectors()
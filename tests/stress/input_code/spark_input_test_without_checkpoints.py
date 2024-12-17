
import time
import psutil
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints_collector import collect_pandera_df_schema


def basic_func():
    start_time = time.time()
    process = psutil.Process()
    memory = process.memory_info().rss / 1024 / 1024
    
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("PerformanceTest")
        .getOrCreate()
    )

    df = spark.createDataFrame(
        [
            ("sue", 32),
            ("li", 3),
            ("bob", 75),
            ("heo", 13),
            ("kathy", 45),
            ("belle", 13),
            ("oona", 9),
            ("jules", 12)
        ],
        ["first_name", "age"],
    )

    # Collect a schema/stats here!
    #collect_pandera_df_schema(df, "performance-test", sample=0.5)
    final_time = time.time() - start_time
    print("Memory: {} Time: {}".format(memory, final_time))
    spark.stop()
    return [start_time, memory, final_time]
    
if __name__ == "__main__":
    basic_func()

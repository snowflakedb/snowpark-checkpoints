#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
import pandas as pd
from src.utils.constants import E2E_INPUT_CSV_PATH

CHECKPOINT_NAME = "test_E2E_initial_checkpoint"
APP_NAME = "E2E_Test"


def input_e2e_test_pyspark(execution_mode: CheckpointMode, temp_path: str) -> None:

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

    data = pd.read_csv(E2E_INPUT_CSV_PATH)
    df = spark.createDataFrame(data)

    collect_dataframe_checkpoint(
        df, CHECKPOINT_NAME, mode=execution_mode, output_path=temp_path
    )

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import time
import pandas as pd
import psutil
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints.checkpoint import validate_dataframe_checkpoint
from snowflake.snowpark_checkpoints.utils.constants import CheckpointMode
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark import Session
from src.utils.constants import STRESS_INPUT_CSV_PATH

CHECKPOINT_NAME = "test_input_collectors_initial_checkpoint"
JOB_NAME = "stress_tests"


def input_validators(execution_mode: CheckpointMode, sample: float, temp_path: str) -> None:

    session = Session.builder.getOrCreate()
    job_context = SnowparkJobContext(
        session, SparkSession.builder.getOrCreate(), JOB_NAME, True
    )

    df = pd.read_csv(STRESS_INPUT_CSV_PATH)
    snowpark_df = session.create_dataframe(df)
    start_time = time.time()
    validate_dataframe_checkpoint(
        snowpark_df,
        CHECKPOINT_NAME,
        job_context,
        mode=execution_mode,
        output_path=temp_path,
    )

    process = psutil.Process()
    memory = process.memory_info().rss / 1024 / 1024
    final_time = time.time() - start_time
    session.close()
    return [round(memory, 2), round(final_time, 2)]

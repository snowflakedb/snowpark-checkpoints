# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import psutil
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
from src.utils.constants import STRESS_INPUT_CSV_PATH

APP_NAME = "stress_tests"
CHECKPOINT_NAME = "test_input_collectors_initial_checkpoint"

def input_collectors(execution_mode: CheckpointMode, sample: float, temp_path: str) -> list:

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    df = spark.read.csv(STRESS_INPUT_CSV_PATH, header=True, inferSchema=True)
    start_time = time.time()
    collect_dataframe_checkpoint(
        df,
        CHECKPOINT_NAME,
        sample=sample,
        mode=execution_mode,
        output_path=temp_path,
    )

    process = psutil.Process()
    memory = process.memory_info().rss / 1024 / 1024
    final_time = time.time() - start_time
    spark.stop()
    return [round(memory, 2), round(final_time, 2)]

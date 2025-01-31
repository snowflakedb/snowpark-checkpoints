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

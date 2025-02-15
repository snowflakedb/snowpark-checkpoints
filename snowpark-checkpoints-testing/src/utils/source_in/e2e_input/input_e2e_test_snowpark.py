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

import pandas as pd
from snowflake.snowpark import Session
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints.utils.constants import CheckpointMode
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.checkpoint import validate_dataframe_checkpoint
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark
from src.utils.constants import E2E_INPUT_CSV_PATH, RESULT_COLUMN_NAME

CHECKPOINT_NAME = "test_E2E_initial_checkpoint"
JOB_NAME = "E2E_Test"
CHECKPOINT_NAME_FUNCTION = "snowpark_function"


def input_e2e_test_snowpark(execution_mode: CheckpointMode, temp_path: str) -> str:
    session = Session.builder.getOrCreate()
    job_context = SnowparkJobContext(
        session, SparkSession.builder.getOrCreate(), JOB_NAME, True
    )
    data = pd.read_csv(E2E_INPUT_CSV_PATH)

    df = session.create_dataframe(data)

    validate_dataframe_checkpoint(
        df, CHECKPOINT_NAME, job_context, mode=execution_mode, output_path=temp_path
    )

    def original_spark_code(df):
        from pyspark.sql.functions import col, when

        ret = df.withColumn(
            RESULT_COLUMN_NAME,
            when(col("INTEGER_TYPE") < 0, "Negative").otherwise("Positive"),
        )

        return ret

    @check_with_spark(
        job_context=job_context,
        spark_function=original_spark_code,
        checkpoint_name=CHECKPOINT_NAME_FUNCTION,
        output_path=temp_path,
        
    )
    def new_snowpark_code(df):
        from snowflake.snowpark.functions import col, lit, when

        ref = df.with_column(
            RESULT_COLUMN_NAME,
            when(col('"INTEGER_TYPE"') < 0, lit("Negative")).otherwise(lit("Positive")),
        )
        return ref

    new_snowpark_code(df)
    return CHECKPOINT_NAME

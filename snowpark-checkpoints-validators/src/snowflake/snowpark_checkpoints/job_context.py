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

import logging

from datetime import datetime
from typing import Optional

import pandas as pd

from pyspark.sql import SparkSession

from snowflake.snowpark import Session
from snowflake.snowpark_checkpoints.utils.constants import SCHEMA_EXECUTION_MODE


LOGGER = logging.getLogger(__name__)
RESULTS_TABLE = "SNOWPARK_CHECKPOINTS_REPORT"


class SnowparkJobContext:

    """Class used to record migration results in Snowflake.

    Args:
        snowpark_session: A Snowpark session instance.
        spark_session: A Spark session instance.
        job_name: The name of the job.
        log_results: Whether to log the migration results in Snowflake.

    """

    def __init__(
        self,
        snowpark_session: Session,
        spark_session: SparkSession = None,
        job_name: Optional[str] = None,
        log_results: Optional[bool] = True,
    ):
        self.log_results = log_results
        self.job_name = job_name
        self.spark_session = spark_session  # A default pyspark session breaks the workflow on snowflake environments.
        self.snowpark_session = snowpark_session

    def _mark_fail(
        self, message, checkpoint_name, data, execution_mode=SCHEMA_EXECUTION_MODE
    ):
        if not self.log_results:
            LOGGER.warning(
                (
                    "Recording of migration results into Snowflake is disabled. "
                    "Failure result for checkpoint '%s' will not be recorded."
                ),
                checkpoint_name,
            )
            return

        LOGGER.debug(
            "Marking failure for checkpoint '%s' in '%s' mode with message '%s'",
            checkpoint_name,
            execution_mode,
            message,
        )

        session = self.snowpark_session
        df = pd.DataFrame(
            {
                "DATE": [datetime.now()],
                "JOB": [self.job_name],
                "STATUS": ["fail"],
                "CHECKPOINT": [checkpoint_name],
                "MESSAGE": [message],
                "DATA": [f"{data}"],
                "EXECUTION_MODE": [execution_mode],
            }
        )
        report_df = session.createDataFrame(df)
        LOGGER.info("Writing failure result to table: '%s'", RESULTS_TABLE)
        report_df.write.mode("append").save_as_table(RESULTS_TABLE)

    def _mark_pass(self, checkpoint_name, execution_mode=SCHEMA_EXECUTION_MODE):
        if not self.log_results:
            LOGGER.warning(
                (
                    "Recording of migration results into Snowflake is disabled. "
                    "Pass result for checkpoint '%s' will not be recorded."
                ),
                checkpoint_name,
            )
            return

        LOGGER.debug(
            "Marking pass for checkpoint '%s' in '%s' mode",
            checkpoint_name,
            execution_mode,
        )

        session = self.snowpark_session
        df = pd.DataFrame(
            {
                "DATE": [datetime.now()],
                "JOB": [self.job_name],
                "STATUS": ["pass"],
                "CHECKPOINT": [checkpoint_name],
                "MESSAGE": [""],
                "DATA": [""],
                "EXECUTION_MODE": [execution_mode],
            }
        )
        report_df = session.createDataFrame(df)
        LOGGER.info("Writing pass result to table: '%s'", RESULTS_TABLE)
        report_df.write.mode("append").save_as_table(RESULTS_TABLE)

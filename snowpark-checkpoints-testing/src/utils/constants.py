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

from typing import Final


# Tables names
SNOWPARK_CHECKPOINTS_REPORT_TABLE_NAME: Final[str] = "SNOWPARK_CHECKPOINTS_REPORT"
SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST_TABLE_NAME: Final[
    str
] = "SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST"

# Columns names
JOB_COLUMN_NAME: Final[str] = "JOB"
STATUS_COLUMN_NAME: Final[str] = "STATUS"
CHECKPOINT_COLUMN_NAME: Final[str] = "CHECKPOINT"
MESSAGE_COLUMN_NAME: Final[str] = "MESSAGE"
DATA_COLUMN_NAME: Final[str] = "DATA"
DATE_COLUMN_NAME: Final[str] = "DATE"
EXECUTION_MODE_COLUMN_NAME: Final[str] = "EXECUTION_MODE"
MEMORY_COLUMN_NAME: Final[str] = "MEMORY"
TIME_COLUMN_NAME: Final[str] = "TIME"
PACKAGE_COLUMN_NAME: Final[str] = "PACKAGE_NAME"
PACKAGE_VERSION_COLUMN_NAME: Final[str] = "PACKAGE_VERSION"
SOURCE_IN_COLUMN_NAME: Final[str] = "SOURCE_IN"
SOURCE_SIZE_COLUMN_NAME: Final[str] = "SOURCE_SIZE"
ERROR_MEMORY_COLUMN_NAME: Final[str] = "ERROR_MEMORY"
ERROR_TIME_COLUMN_NAME: Final[str] = "ERROR_TIME"
EXECUTION_DATE_COLUMN_NAME: Final[str] = "EXECUTION_DATE"
RESULT_COLUMN_NAME: Final[str] = "RESULT"

# Queries
SQL_CREATE_PERFORMANCE_TABLE = (
    f"CREATE OR REPLACE TABLE {SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST_TABLE_NAME}"
    f"({EXECUTION_DATE_COLUMN_NAME} TIMESTAMP_TZ,"
    f"{PACKAGE_COLUMN_NAME} VARCHAR, "
    f"{PACKAGE_VERSION_COLUMN_NAME} VARCHAR, "
    f"{SOURCE_IN_COLUMN_NAME} VARCHAR, "
    f"{SOURCE_SIZE_COLUMN_NAME} VARCHAR, "
    f"{EXECUTION_MODE_COLUMN_NAME} VARCHAR, "
    f"{MEMORY_COLUMN_NAME} FLOAT, "
    f"{TIME_COLUMN_NAME} FLOAT, "
    f"{ERROR_MEMORY_COLUMN_NAME} VARCHAR, "
    f"{ERROR_TIME_COLUMN_NAME} VARCHAR);"
)

# Paths
STRESS_INPUT_CSV_PATH: Final[
    str
] = "snowpark-checkpoints-testing/src/utils/source_in/stress_input/data_input_medium.csv"
E2E_INPUT_CSV_PATH: Final[
    str
] = "snowpark-checkpoints-testing/src/utils/source_in/e2e_input/data_e2e_test.csv"
SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME: Final[str] = "snowpark-checkpoints-output"

# Packages names
PACKAGE_NAME_COLLECTORS: Final[str] = "collectors"
PACKAGE_NAME_VALIDATORS: Final[str] = "validators"

# Limits keys
LIMIT_SUP_MEMORY_KEY: Final[str] = "sup_memory"
LIMIT_SUP_TIME_KEY: Final[str] = "sup_time"

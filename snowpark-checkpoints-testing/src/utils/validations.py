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

import os
import json
import re
from src.utils.utils import get_version
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark_checkpoints.utils.constants import (
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    CheckpointMode,
)
from src.utils.constants import (
    SNOWPARK_CHECKPOINTS_REPORT_TABLE_NAME,
    JOB_COLUMN_NAME,
    STATUS_COLUMN_NAME,
    CHECKPOINT_COLUMN_NAME,
    MESSAGE_COLUMN_NAME,
    DATA_COLUMN_NAME,
    EXECUTION_MODE_COLUMN_NAME,
    DATE_COLUMN_NAME,
)

MESSAGE_VALUE = "message"
EVENT_NAME_VALUE = "event_name"
TYPE_VALUE = "type"
METADATA_VALUE = "metadata"
DATA_VALUE = "data"
JOB_NAME = "E2E_Test"
SNOWPARK_CHECKPOINTS_VERSION_VALUE = "snowpark_checkpoints_version"

expected_data = {
    "Schema": [
        ["E2E_Test", "pass", "snowpark_function", "", "", "Schema"],
        ["E2E_Test", "pass", "test_E2E_initial_checkpoint", "", "", "Schema"],
    ],
    "Dataframe": [
        ["E2E_Test", "pass", "snowpark_function", "", "", "Schema"],
        ["E2E_Test", "pass", "test_E2E_initial_checkpoint", "", "", "Dataframe"],
    ],
}

data_expected_telemetry = {
    1: [
        {
            "event_name": "DataFrame_Collection_Schema",
            "snowpark_checkpoints_version": "0.1.0rc3",
            "type": "info",
            "data": '{"function": "_collect_dataframe_checkpoint_mode_schema", "mode": 1, "schema_types": ["long", "string", "long", "long", "double", "double", "boolean", "string", "string"]}',
        },
        {
            "event_name": "DataFrame_Validator_Schema",
            "snowpark_checkpoints_version": "0.1.0rc3",
            "type": "info",
            "data": '{"function": "_check_dataframe_schema", "mode": 1, "status": true, "schema_types": ["int64", "object", "int64", "int64", "float64", "float64", "bool", "object", "object"]}',
        },
        {
            "event_name": "DataFrame_Validator_Mirror",
            "snowpark_checkpoints_version": "0.1.0rc3",
            "type": "info",
            "data": '{"function": "_assert_return", "status": true, "snowflake_schema_types": ["LongType()", "StringType()", "LongType()", "LongType()", "DoubleType()", "DoubleType()", "BooleanType()", "StringType()", "StringType()", "StringType(8)"], "spark_schema_types": ["LongType()", "StringType()", "LongType()", "LongType()", "DoubleType()", "DoubleType()", "BooleanType()", "StringType()", "StringType()", "StringType()"]}',
        },
    ],
    2: [
        {
            "event_name": "DataFrame_Collection_DF",
            "snowpark_checkpoints_version": "0.1.0rc3",
            "type": "info",
            "data": '{"function": "_collect_dataframe_checkpoint_mode_dataframe", "mode": 2, "spark_schema_types": ["LongType()", "StringType()", "LongType()", "LongType()", "DoubleType()", "DoubleType()", "BooleanType()", "StringType()", "StringType()"]}',
        },
        {
            "event_name": "DataFrame_Validator_DF",
            "snowpark_checkpoints_version": "0.1.0rc3",
            "type": "info",
            "data": '{"function": "_compare_data", "mode": 2, "status": true, "schema_types": ["LongType()", "StringType()", "LongType()", "LongType()", "DoubleType()", "DoubleType()", "BooleanType()", "StringType()", "StringType()"]}',
        },
        {
            "event_name": "DataFrame_Validator_Mirror",
            "snowpark_checkpoints_version": "0.1.0rc3",
            "type": "info",
            "data": '{"function": "_assert_return", "status": true, "snowflake_schema_types": ["LongType()", "StringType()", "LongType()", "LongType()", "DoubleType()", "DoubleType()", "BooleanType()", "StringType()", "StringType()", "StringType(8)"], "spark_schema_types": ["LongType()", "StringType()", "LongType()", "LongType()", "DoubleType()", "DoubleType()", "BooleanType()", "StringType()", "StringType()", "StringType()"]}',
        },
    ],
}

expected_columns = [
    DATE_COLUMN_NAME,
    JOB_COLUMN_NAME,
    STATUS_COLUMN_NAME,
    CHECKPOINT_COLUMN_NAME,
    MESSAGE_COLUMN_NAME,
    DATA_COLUMN_NAME,
    EXECUTION_MODE_COLUMN_NAME,
]


def validate_json_file_generated(json_name_list: list, temp_path: str) -> None:
    """
    Validates that JSON files listed in json_name_list are generated and exist in the specified output directory.

    Args:
        json_name_list (list): A list of JSON file names to be validated.
        temp_path (str): The temporary path where the JSON files are expected to be found.

    Raises:
        AssertionError: If any of the JSON files do not exist in the specified directory.
    """
    for json_file in json_name_list:
        path_file = os.path.join(
            temp_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME, json_file
        )
        assert os.path.isfile(
            path_file
        ), f"File {json_file} does not exist in the directory {SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME}"


def validate_columns_table_checkpoints_results(df: pd.DataFrame) -> bool:
    """
    Validates that the DataFrame `df` contains the expected columns for the 'SNOWPARK_CHECKPOINTS_REPORT' table.

    Args:
        df (pandas.DataFrame): DataFrame to validate.

    Returns:
        bool: True if the DataFrame contains the expected columns, False otherwise.

    Raises:
        AssertionError: If the DataFrame does not contain the expected columns.
    """

    assert all(
        col in df.columns for col in expected_columns
    ), "The DataFrame does not contain the expected columns"


def validate_checkpoints_results_table_generated() -> pd.DataFrame:
    """
    Validates the 'SNOWPARK_CHECKPOINTS_REPORT' table and returns the latest two entries for the 'E2E_Test' job.
    This function performs the following steps:
    1. Creates a session and retrieves the 'SNOWPARK_CHECKPOINTS_REPORT' table as a pandas DataFrame.
    2. Checks if the DataFrame is empty and raises an assertion error if it is.
    3. Validates the columns of the DataFrame using the 'validate_columns_table_checkpoints_results' function and raises an assertion error if the columns are not as expected.
    4. Retrieves the latest two entries for the 'E2E_Test' job, ordered by the 'Date' column in descending order, and returns them as a pandas DataFrame.
    5. Closes the session.
    Returns:
        pandas.DataFrame: The latest two entries for the 'E2E_Test' job from the 'SNOWPARK_CHECKPOINTS_REPORT' table.
    Raises:
        AssertionError: If the 'SNOWPARK_CHECKPOINTS_REPORT' table is empty or does not contain the expected columns.
    """

    df = None
    session = Session.builder.getOrCreate()

    try:
        df = (
            session.table(SNOWPARK_CHECKPOINTS_REPORT_TABLE_NAME).select("*").toPandas()
        )
        if df.empty:
            assert False, f"The table {SNOWPARK_CHECKPOINTS_REPORT_TABLE_NAME} is empty"
        elif validate_columns_table_checkpoints_results(df) == False:
            assert (
                False
            ), f"The table {SNOWPARK_CHECKPOINTS_REPORT_TABLE_NAME} does not contain the expected columns"
    finally:
        df_output = (
            session.table(SNOWPARK_CHECKPOINTS_REPORT_TABLE_NAME)
            .select(
                JOB_COLUMN_NAME,
                STATUS_COLUMN_NAME,
                CHECKPOINT_COLUMN_NAME,
                MESSAGE_COLUMN_NAME,
                DATA_COLUMN_NAME,
                EXECUTION_MODE_COLUMN_NAME,
            )
            .where(col(JOB_COLUMN_NAME) == lit(JOB_NAME))
            .orderBy(col(DATE_COLUMN_NAME).desc())
            .limit(2)
            .toPandas()
        )
        session.close()
        return df_output


def validate_output_checkpoints_results_table(
    df: pd.DataFrame, execution_mode: str
) -> None:
    """
    Validates that the given DataFrame matches the expected output checkpoints results table.

    Args:
        df (pd.DataFrame): The DataFrame to validate.
        execution_mode (str): The execution mode to use for validation.

    Raises:
        AssertionError: If the DataFrame does not match the expected data.
    """

    df_expected = pd.DataFrame(
        expected_data[execution_mode],
        columns=[
            JOB_COLUMN_NAME,
            STATUS_COLUMN_NAME,
            CHECKPOINT_COLUMN_NAME,
            MESSAGE_COLUMN_NAME,
            DATA_COLUMN_NAME,
            EXECUTION_MODE_COLUMN_NAME,
        ],
    )
    assert df_expected.equals(df), "The output table does not match the expected data"


def validate_telemetry_data(execution_mode: CheckpointMode, temp_path: str) -> None:
    """
    Validates telemetry data files against expected data for a given execution mode.
    Args:
        execution_mode (CheckpointMode): The mode of execution to validate telemetry data for.
    Raises:
        AssertionError: If any of the telemetry data does not match the expected values.
    This function performs the following steps:
    1. Constructs the path to the telemetry data directory.
    2. Compiles a regex pattern to match JSON telemetry files.
    3. Walks through the directory to find all telemetry files matching the pattern.
    4. Sorts the list of telemetry files.
    5. Iterates through each telemetry file and compares its contents with the expected data.
    6. Asserts that the event name, snowpark checkpoints version, event type, and event data match the expected values.
    """
    result = temp_path.glob("*.json")
    result_list = list(result)
    result_list.sort()
    for count in range(len(result_list)):
        file = result_list[count]
        data_expected = data_expected_telemetry[execution_mode.value][count]
        with open(file, "r") as file:
            data = json.load(file)
            assert (
                data[MESSAGE_VALUE][EVENT_NAME_VALUE] == data_expected[EVENT_NAME_VALUE]
            ), "Telemetry: The event name is not correct"
            assert (
                data[MESSAGE_VALUE][METADATA_VALUE][SNOWPARK_CHECKPOINTS_VERSION_VALUE]
                == get_version()
            ), "Telemetry: The snowpark checkpoints version is not correct"
            assert (
                data[MESSAGE_VALUE][TYPE_VALUE] == data_expected[TYPE_VALUE]
            ), "Telemetry: The event type is not correct"
            assert (
                data[MESSAGE_VALUE][DATA_VALUE] == data_expected[DATA_VALUE]
            ), "Telemetry: The event data is not correct"

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from datetime import datetime
from tests.src.utils.source_in.stress_input.input_validators import input_validators
from tests.src.utils.source_in.stress_input.input_collectors import input_collectors
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit
from __version__ import __version__ as package_version
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
from src.utils.constants import (
    SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST_TABLE_NAME,
    MEMORY_COLUMN_NAME,
    TIME_COLUMN_NAME,
    PACKAGE_COLUMN_NAME,
    SOURCE_IN_COLUMN_NAME,
    EXECUTION_MODE_COLUMN_NAME,
    EXECUTION_DATE_COLUMN_NAME,
    SQL_QUERY_INSERT_PERFORMANCE_TEST,
    LIMIT_SUP_MEMORY_KEY,
    LIMIT_SUP_TIME_KEY,
)

input_functions = {
    "input_validators": input_validators,
    "input_collectors": input_collectors,
}

execution_mode_name = {"1": "Schema", "2": "Dataframe"}

sample = {"1": 0.3, "2": 1.0}


def performance_test(
    package_name: str,
    file_name: str,
    size_file: str,
    execution_mode: CheckpointMode,
    limits: dict,
    temp_path: str,
) -> None:
    """
    Executes a performance test for a given package and file, and logs the results in a Snowflake table.

    Parameters:
    package_name (str): The name of the package being tested (Collectors or Validators).
    file_name (str): The name of the file being tested.
    size_file (str): The input size of the file being tested.
    execution_mode (CheckpointMode): The mode in which the test is executed (Schema or Dataframe).
    limits (dict): A dictionary containing the memory and time limits for the test.
    temp_path (str): The temporary path for storing intermediate results.

    Raises:
    AssertionError: If the memory consumption or execution time exceeds the specified limits.
    """

    error_memory, error_time = None, None
    memory, time = input_functions[file_name](
        execution_mode, sample[str(execution_mode.value)], temp_path
    )
    session = Session.builder.getOrCreate()
    last_record = (
        session.table(SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST_TABLE_NAME)
        .select(MEMORY_COLUMN_NAME, TIME_COLUMN_NAME)
        .where(col(PACKAGE_COLUMN_NAME) == lit(package_name))
        .where(col(SOURCE_IN_COLUMN_NAME) == lit(file_name))
        .where(
            col(EXECUTION_MODE_COLUMN_NAME)
            == lit(execution_mode_name[str(execution_mode.value)])
        )
        .orderBy(col(EXECUTION_DATE_COLUMN_NAME).desc())
        .limit(1)
        .toPandas()
    )

    if not last_record.empty:
        value_last_record_memory = last_record[MEMORY_COLUMN_NAME][0]
        value_last_record_time = last_record[TIME_COLUMN_NAME][0]
        error_memory = (
            f"Error - Increased memory consumption in this version (Diference: {round((memory-value_last_record_memory),2)} MB)"
            if memory > value_last_record_memory
            else None
        )
        error_time = (
            f"Error - Increased time execution in this version (Diference: {round((time-value_last_record_time),2)} seconds)"
            if time > value_last_record_time
            else None
        )
    sql = SQL_QUERY_INSERT_PERFORMANCE_TEST

    values = [
        datetime.now(),
        package_name,
        package_version,
        file_name,
        size_file,
        execution_mode_name[str(execution_mode.value)],
        memory,
        time,
        error_memory,
        error_time,
    ]
    session.sql(sql, params=values).collect()
    session.close()
    memory_within_limits = memory < limits[package_name][LIMIT_SUP_MEMORY_KEY]
    time_within_limits = time < limits[package_name][LIMIT_SUP_TIME_KEY]

    if not memory_within_limits:
        assert (
            False
        ), f"Memory consumption {memory}MB is out of limit. Expected less than {limits[package_name][LIMIT_SUP_MEMORY_KEY]}MB."

    if not time_within_limits:
        assert (
            False
        ), f"Execution time {time}seconds is out of limits. Expected less than {limits[package_name][LIMIT_SUP_TIME_KEY]}seconds."

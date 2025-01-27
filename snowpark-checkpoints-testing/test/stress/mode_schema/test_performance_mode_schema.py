#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import pytest
from snowflake.snowpark_checkpoints.utils.telemetry import (
    TelemetryManager,
    get_telemetry_manager,
)
from snowflake.snowpark_checkpoints.utils.constants import CheckpointMode
from snowflake.snowpark_checkpoints_collector.singleton import Singleton as SingletonCollector
from snowflake.snowpark_checkpoints.singleton import Singleton as SingletonValidator
from performance_test import performance_test
import tempfile
from os import getcwd
from pathlib import Path
from src.utils.constants import PACKAGE_NAME_COLLECTORS, PACKAGE_NAME_VALIDATORS


EXECUTION_MODE = CheckpointMode.SCHEMA
SIZE = "medium"


input_name = {
    "collectors": "input_collectors",
    "validators": "input_validators",
}

limits_mode_schema = {
    "validators": {
        "sup_memory": 1510.00,
        "sup_time": 20.00,
    },
    "collectors": {
        "sup_memory": 1200.00,
        "sup_time": 80.00,
    },
}


@pytest.fixture(autouse=True)
def singleton():
    SingletonCollector._instances = {}
    SingletonValidator._instances = {}


@pytest.fixture
def telemetry():
    return get_telemetry_manager()


def test_performance_mode_schema(telemetry: TelemetryManager) -> None:
    """
    Test the performance of the schema execution mode.

    This function creates a temporary directory within the 'tests' directory and runs performance tests
    for both collectors and validators using the specified parameters.

    The performance tests are executed with the following parameters:
    - PACKAGE_NAME_COLLECTORS: The package name for collectors.
    - PACKAGE_NAME_VALIDATORS: The package name for validators.
    - input_name: A dictionary containing input names for collectors and validators.
    - SIZE: The input size parameter for the performance test.
    - EXECUTION_MODE: The execution mode for the performance test.
    - limits_mode_schema: A dictionary containing the memory and time limits in schema execution mode for the test.
    - temp_path: The path to the temporary directory.

    The temporary directory is automatically cleaned up after the tests are completed.
    """
    with tempfile.TemporaryDirectory(
        dir=(os.path.join(getcwd()))
    ) as temp_dir:
        temp_path = Path(temp_dir)
        telemetry.set_sc_output_path(temp_path)
        performance_test(
            PACKAGE_NAME_COLLECTORS,
            input_name[PACKAGE_NAME_COLLECTORS],
            SIZE,
            EXECUTION_MODE,
            limits_mode_schema,
            temp_path,
        )
        performance_test(
            PACKAGE_NAME_VALIDATORS,
            input_name[PACKAGE_NAME_VALIDATORS],
            SIZE,
            EXECUTION_MODE,
            limits_mode_schema,
            temp_path,
        )

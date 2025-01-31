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
import tempfile
from os import getcwd
from pathlib import Path
import pytest
from snowflake.snowpark_checkpoints.utils.constants import CheckpointMode
from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager, get_telemetry_manager
from snowflake.snowpark_checkpoints_collector.singleton import Singleton as SingletonCollector
from snowflake.snowpark_checkpoints.singleton import Singleton as SingletonValidator
from src.utils.constants import LIMIT_SUP_MEMORY_KEY, LIMIT_SUP_TIME_KEY, PACKAGE_NAME_COLLECTORS, PACKAGE_NAME_VALIDATORS
from performance_test import performance_test

EXECUTION_MODE = CheckpointMode.DATAFRAME
SIZE = "medium"


input_name = {
    PACKAGE_NAME_COLLECTORS: "input_collectors",
    PACKAGE_NAME_VALIDATORS: "input_validators",
}

limits_mode_dataframe = {
    PACKAGE_NAME_VALIDATORS: {
        LIMIT_SUP_MEMORY_KEY : 900.00,
        LIMIT_SUP_TIME_KEY: 20.00,
    },
    PACKAGE_NAME_COLLECTORS: {
        LIMIT_SUP_MEMORY_KEY: 520.00,
        LIMIT_SUP_TIME_KEY: 45.00,
    },
}

@pytest.fixture(autouse=True)
def singleton():
    SingletonCollector._instances = {}
    SingletonValidator._instances = {}

@pytest.fixture
def telemetry():
    return get_telemetry_manager()

def test_performance_mode_dataframe(telemetry: TelemetryManager) -> None:
    """
    Test the performance of the execution mode dataframe.

    This function creates a temporary directory within the 'tests' directory and runs performance tests
    for both collectors and validators using the specified parameters.

    Parameters:
    - telemetry (TelemetryManager): The telemetry manager instance.
    
    The performance tests are executed with the following parameters:
    - PACKAGE_NAME_COLLECTORS: The package name for collectors.
    - PACKAGE_NAME_VALIDATORS: The package name for validators.
    - input_name: A dictionary containing input names for collectors and validators.
    - SIZE: The input size parameter for the performance test.
    - EXECUTION_MODE: The execution mode for the performance test.
    - limits_mode_dataframe: A dictionary containing the memory and time limits in dataframe execution mode for the test.
    - temp_path: The path to the temporary directory.

    The temporary directory is automatically cleaned up after the tests are completed.
    """
    with tempfile.TemporaryDirectory(dir=(os.path.join(getcwd()))) as temp_dir:
        temp_path = Path(temp_dir)
        telemetry.set_sc_output_path(temp_path)
        performance_test(PACKAGE_NAME_COLLECTORS,input_name[PACKAGE_NAME_COLLECTORS],SIZE,EXECUTION_MODE, limits_mode_dataframe, temp_path)
        performance_test(PACKAGE_NAME_VALIDATORS,input_name[PACKAGE_NAME_VALIDATORS],SIZE,EXECUTION_MODE, limits_mode_dataframe, temp_path)

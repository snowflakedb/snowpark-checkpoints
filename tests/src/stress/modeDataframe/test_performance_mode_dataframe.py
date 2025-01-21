import os
import tempfile
from os import getcwd
from pathlib import Path
import pytest
from snowflake.snowpark_checkpoints.utils.telemetry import get_telemetry_manager
from snowflake.snowpark_checkpoints_collector.singleton import Singleton

from tests.performance_test import performance_test

EXECUTION_MODE = "Dataframe"
SIZE = "medium"
PACKAGE_NAME_COLLECTORS = "collectors"
PACKAGE_NAME_VALIDATORS = "validators"

input_name = {
    "collectors": "Input_collectors",
    "validators": "Input_validators",
}

limits_mode_dataframe = {
    "validators": {
        "sup_memory": 900.00,
        "sup_time": 20.00,
    },
    "collectors": {
        "sup_memory": 520.00,
        "sup_time": 45.00,
    },
}

@pytest.fixture
def singleton():
    Singleton._instances = {}

@pytest.fixture
def telemetry():
    return get_telemetry_manager()

def test_performance_mode_dataframe(telemetry,singleton):
    """
    Test the performance of the exucution mode dataframe.

    This function creates a temporary directory within the 'tests' directory and runs performance tests
    for both collectors and validators using the specified parameters.

    The performance tests are executed with the following parameters:
    - PACKAGE_NAME_COLLECTORS: The package name for collectors.
    - PACKAGE_NAME_VALIDATORS: The package name for validators.
    - input_name: A dictionary containing input names for collectors and validators.
    - SIZE: The input size parameter for the performance test.
    - EXECUTION_MODE: The execution mode for the performance test.
    - limits_mode_dataframe: A dictionary containing the memory and time limits in dataframe execution mode  for the test.
    - temp_path: The path to the temporary directory.

    The temporary directory is automatically cleaned up after the tests are completed.
    """
    with tempfile.TemporaryDirectory(dir=(os.path.join(getcwd(),"tests"))) as temp_dir:
        temp_path = Path(temp_dir)
        telemetry.set_sc_output_path(temp_path)
        performance_test(PACKAGE_NAME_COLLECTORS,input_name[PACKAGE_NAME_COLLECTORS],SIZE,EXECUTION_MODE, limits_mode_dataframe, temp_path)
        performance_test(PACKAGE_NAME_VALIDATORS,input_name[PACKAGE_NAME_VALIDATORS],SIZE,EXECUTION_MODE, limits_mode_dataframe, temp_path)


        
if __name__ == "__main__":
    test_performance_mode_dataframe()   
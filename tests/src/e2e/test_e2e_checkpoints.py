#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
from pathlib import Path
import tempfile
import pytest
from snowflake.snowpark_checkpoints.utils.constants import CheckpointMode
from snowflake.snowpark_checkpoints_collector.singleton import Singleton
from tests.src.utils.constants import TESTS_FOLDER_NAME


from tests.src.utils.source_in.e2e_input.input_e2e_test_pyspark import (
    input_e2e_test_pyspark,
)
from tests.src.utils.source_in.e2e_input.input_e2e_test_snowpark import (
    input_e2e_test_snowpark,
)
from src.utils.validations import (
    validate_json_file_generated,
    validate_output_checkpoints_results_table,
    validate_checkpoints_results_table_generated,
)
from snowflake.snowpark_checkpoints.utils.telemetry import get_telemetry_manager


testdata = [(["test_E2E_initial_checkpoint.json", "checkpoint_collection_results.json", "checkpoint_validation_results.json"], CheckpointMode.SCHEMA),
            (["checkpoint_collection_results.json", "checkpoint_validation_results.json"], CheckpointMode.DATAFRAME)]

execution_mode_name = {"1": "Schema", "2": "Dataframe"}

@pytest.fixture(autouse=True)
def singleton():
    Singleton._instances = {}


@pytest.fixture
def telemetry():
    return get_telemetry_manager()


@pytest.mark.parametrize("json_name_list, execution_mode", testdata)
def test_e2e_checkpoints(json_name_list, execution_mode, telemetry) -> None:
    """
    End-to-end test for collectors and validators in mode Dataframe and Schema.

    This test function performs the following steps:
    1. Executes the input_e2e_test_pyspark function.
    2. Executes the input_e2e_test_snowpark function.
    3. Validates that the specified JSON files are generated.
    4. Validates that the checkpoints results table is generated and returns the DataFrame.
    5. Validates the output checkpoints results table using the returned DataFrame.

    Args:
        json_name_list (list): A list of JSON file names to validate.
        execution_mode (CheckpointMode): The mode of execution for the test.
        telemetry: The telemetry manager instance.
    """
    with tempfile.TemporaryDirectory(
        dir=(os.path.join(os.getcwd(), TESTS_FOLDER_NAME))
    ) as temp_dir:
        temp_path = Path(temp_dir)
        telemetry.set_sc_output_path(temp_path)
        input_e2e_test_pyspark(execution_mode, temp_path)
        input_e2e_test_snowpark(execution_mode, temp_path)
        validate_json_file_generated(json_name_list, temp_path)
        df = validate_checkpoints_results_table_generated()
        validate_output_checkpoints_results_table(df, execution_mode_name[str(execution_mode.value)])

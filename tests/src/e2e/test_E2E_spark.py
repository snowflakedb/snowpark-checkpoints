import os
from pathlib import Path
import tempfile
import pytest
from snowflake.snowpark_checkpoints_collector.singleton import Singleton


from tests.src.utils.source_in.e2e_input.input_E2E_test_Pyspark import input_E2E_test_Pyspark
from tests.src.utils.source_in.e2e_input.input_E2E_test_Snowpark import input_E2E_test_Snowpark
from src.utils.validations import validateJSONFileGenerated, validateOutputCheckpointsResultsTable, validateCheckpointsResultsTableGenerated
from snowflake.snowpark_checkpoints.utils.telemetry import (get_telemetry_manager)



# Act
testdata = [(["test_E2E_initial_checkpoint.json"], "Schema")]

@pytest.fixture
def singleton():
    Singleton._instances = {}
    
    
@pytest.fixture
def telemetry():
    return get_telemetry_manager()
    

@pytest.mark.parametrize("JsonNameList, execution_mode", testdata)
def test_E2E_spark(JsonNameList, execution_mode, telemetry, singleton):
    """
    End-to-end test for Spark and Snowpark pipelines.

    This test function performs the following steps:
    1. Executes the input_E2E_test_Pyspark.
    2. Executes the input_E2E_test_Snowpark.
    3. Validates that the JSON file is generated using the provided JsonNameList.
    4. Validates that the table SNOWPARK_CHECKPOINTS_REPORT is generated and return the DataFrame.
    5. Validates the output table using the returned DataFrame.

    Args:
        JsonNameList (list): A list of JSON file names to validate.
    """
    with tempfile.TemporaryDirectory(dir=(os.path.join(os.getcwd(),"tests"))) as temp_dir:
        temp_path = Path(temp_dir)
        telemetry.set_sc_output_path(temp_path)
        input_E2E_test_Pyspark(execution_mode,temp_path)
        input_E2E_test_Snowpark(execution_mode,temp_path)
        validateJSONFileGenerated(JsonNameList, temp_path)
        df = validateCheckpointsResultsTableGenerated()
        validateOutputCheckpointsResultsTable(df)
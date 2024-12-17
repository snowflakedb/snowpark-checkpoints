import pytest
import sys

#sys.path.insert(2, 'test/utils/source_in')
#sys.path.insert(3, 'snowpark-checkpoints-validators/src/snowflake/snowpark-checkpoints/src')
from validations import validateJSONFileGenerated, validateOutputTable, validateTableGenerated
from demo_pyspark_pipeline import demo_pyspark_pipeline
from demo_snowpark_pipeline import demo_snowpark_pipeline

testdata = [(["demo-initial-creation-checkpoint.json","demo-add-a-column.json"])]


@pytest.mark.parametrize("JsonNameList", testdata)
def test_E2E_spark(JsonNameList):
    """
    End-to-end test for Spark and Snowpark pipelines.

    This test function performs the following steps:
    1. Executes the XXXx.
    2. Executes the XXXX.
    3. Validates that the JSON file is generated using the provided JsonNameList.
    4. Validates that the table SNOWPARK_CHECKPOINTS_REPORT is generated and return the DataFrame.
    5. Validates the output table using the returned DataFrame.

    Args:
        JsonNameList (list): A list of JSON file names to validate.
    """
    demo_pyspark_pipeline()
    demo_snowpark_pipeline()
    validateJSONFileGenerated(JsonNameList)
    df = validateTableGenerated()
    validateOutputTable(df)

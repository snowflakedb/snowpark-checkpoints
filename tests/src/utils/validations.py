import os
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit

SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME = 'snowpark-checkpoints-output'

dataExpected = [[ 'E2E_Test', 'pass', 'snowpark_function', '', '','Schema'],
                [ 'E2E_Test', 'pass', 'test_E2E_initial_checkpoint', '', '','Schema']]

def validateJSONFileGenerated(jsonNameList, temp_path):
    """
    Validates that JSON files listed in jsonNameList are generated and exist in the specified output directory.

    Args:
        jsonNameList (list): A list of JSON file names to be validated.

    Raises:
        AssertionError: If any of the JSON files do not exist in the specified directory.
    """
    for jsonName in jsonNameList:
        path_file = os.path.join(temp_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME, jsonName)
        assert os.path.isfile(path_file), f"File {jsonName} does not exist in the directory {SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME}"

def validateColumnsTableCheckpointsResults(df):
    """
    Validates the columns of the DataFrame `df` containing the table 'SNOWPARK_CHECKPOINTS_REPORT'.

    Args:
        df (pandas.DataFrame): DataFrame containing the table 'SNOWPARK_CHECKPOINTS_REPORT'.

    Raises:
        AssertionError: If the DataFrame `df` does not contain the expected columns.
    """
    expectedColumns = ['DATE','JOB', 'STATUS', 'CHECKPOINT', 'MESSAGE', 'DATA','EXECUTION_MODE']
    assert all(col in df.columns for col in expectedColumns), "The DataFrame does not contain the expected columns"

def validateCheckpointsResultsTableGenerated():
    """
    Validates the 'SNOWPARK_CHECKPOINTS_REPORT' table and returns the latest two entries for the 'E2E_Test' job.
    This function performs the following steps:
    1. Creates a session and retrieves the 'SNOWPARK_CHECKPOINTS_REPORT' table as a pandas DataFrame.
    2. Checks if the DataFrame is empty and raises an assertion error if it is.
    3. Validates the columns of the DataFrame using the 'validateColumnsTableCheckpointsResults' function and raises an assertion error if the columns are not as expected.
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
        df = session.table("SNOWPARK_CHECKPOINTS_REPORT").select("*").toPandas()
        if df.empty:
           assert False, "The table 'SNOWPARK_CHECKPOINTS_REPORT' is empty"
        elif validateColumnsTableCheckpointsResults(df) == False:
            assert False, "The table 'SNOWPARK_CHECKPOINTS_REPORT' does not contain the expected columns"
    finally:
        df_output= (session.table("SNOWPARK_CHECKPOINTS_REPORT")
                    .select('JOB', 'STATUS', 'CHECKPOINT', 'MESSAGE', 'DATA','EXECUTION_MODE')
                    .where(col('JOB')==lit('E2E_Test'))
                    .orderBy(col('Date').desc())
                    .limit(2)
                    .toPandas())
        session.close() 
        return df_output

def validateOutputCheckpointsResultsTable(df):
    """
    Validates that the given DataFrame matches the expected output checkpoints results table.
    Args:
        df (pd.DataFrame): The DataFrame to validate.
    Raises:
        AssertionError: If the DataFrame does not match the expected data.
    """
    
    dfExpected = pd.DataFrame(dataExpected, columns=[ 'JOB', 'STATUS', 'CHECKPOINT', 'MESSAGE', 'DATA','EXECUTION_MODE'])
    assert dfExpected.equals(df), "The output table does not match the expected data"
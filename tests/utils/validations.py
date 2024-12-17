import os
import pandas as pd
import snowflake.connector

from connection import connectionConfig  # Ensure this import is correct and the function exists

SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME = 'snowpark-checkpoints-output'

dataExpected = [[ 'realdemo', 'pass', 'demo-initial-creation-checkpoint', '', ''],
            [ 'realdemo', 'pass', 'new_snowpark_code_I_do_understand', '', ''],
            [ 'realdemo', 'pass', 'demo-add-a-column', '', '']]

def validateJSONFileGenerated(jsonNameList):
    """
    Validates that JSON files listed in jsonNameList are generated and exist in the specified output directory.

    Args:
        jsonNameList (list): A list of JSON file names to be validated.

    Raises:
        AssertionError: If any of the JSON files do not exist in the specified directory.
    """
    for jsonName in jsonNameList:
        path_file = os.path.join(os.getcwd(), SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME, jsonName)
        assert os.path.isfile(path_file), f"File {jsonName} does not exist in the directory {SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME}"

def validateTableGenerated():
    """
    Validates that the table 'SNOWPARK_CHECKPOINTS_REPORT' in the Snowflake database is not empty.

    This function connects to a Snowflake database using connection parameters defined in the 
    `connection.connectionConfig` dictionary. It then executes a query to select all records 
    from the 'SNOWPARK_CHECKPOINTS_REPORT' table and fetches the results into a pandas DataFrame. 
    An assertion is made to ensure that the DataFrame is not empty.

    Raises:
        AssertionError: If the 'SNOWPARK_CHECKPOINTS_REPORT' table is empty.
    """
    df = pd.DataFrame()
    connectionParameters = connectionConfig()
    conn = snowflake.connector.connect(
        user = connectionParameters['connection']['user'],
        password=connectionParameters['connection']['password'],
        account=connectionParameters['connection']['account'],
        warehouse=connectionParameters['connection']['warehouse'],
        database=connectionParameters['connection']['database'],
        schema=connectionParameters['connection']['schema'],
        role=connectionParameters['connection']['role']
    )

    try:
        df = conn.cursor().execute("SELECT * FROM SNOWPARK_CHECKPOINTS_REPORT").fetch_pandas_all()
        assert not df.empty, "The table 'SNOWPARK_CHECKPOINTS_REPORT' is empty"
        
    finally:
        conn.close()
        return df

def validateOutputTable(df):
    """
    Validates the output table by comparing it with the expected data.
    The function creates a DataFrame with the expected data and compares it 
    with the actual DataFrame `df`. If the DataFrames are not equal, an 
    assertion error is raised.
    Raises:
        AssertionError: If the actual DataFrame `df` does not match the expected DataFrame.
    """
    
    df = df.drop(columns='DATE', errors='ignore')
    dfExpected = pd.DataFrame(dataExpected, columns=[ 'JOB', 'STATUS', 'CHECKPOINT', 'MESSAGE', 'DATA'])
    assert dfExpected.equals(df)
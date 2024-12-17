import os
import sys
import pytest
import subprocess
import snowflake.connector
sys.path.insert(1, 'snowpark-checkpoints-validators/test/utils')
from connection import connectionConfig

testdata = [("spark_input_test_with_checkpoints.py" )]

def connect_snowflake():
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
    return conn

@pytest.mark.parametrize("input_code", testdata)
def test_performance_func(input_code):
    current_path = os.getcwd()
    folder_path = os.path.join(current_path, "snowpark-checkpoints-validators", "test", "stress", "input_code")
    result = subprocess.run(["python", os.path.join(folder_path, input_code)], capture_output=True, text=True, check=True, env=env)
    output = result.stdout.strip().split()
    errorMemory, errorTime = None, None
    if len(output) == 4:
        memory, time = float(output[1]), float(output[3])
        conn = connect_snowflake()
        df = conn.cursor().execute("SELECT * FROM SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST").fetch_pandas_all()
        if len(df) > 0:
            for row in len(df):
                if memory > df["memory"].iloc[row]:
                    errorMemory = f"Error- Increased memory consumption in this version"
                if time > df["time"].iloc[row]:
                    errorTime = f"Error- Increased time execution in this version"
            if errorMemory is not None or errorTime is not None:
                assert False, f"Error: Check the table SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST for more details"
        sql = "INSERT INTO SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST ( VERSION, MEMORY, TIME, ERRORMEMORY, ERRORTIME ) VALUES (%s,%s,%s,%s,%s)"
        values = ("", memory, time, errorMemory, errorTime)
        conn.cursor().execute(sql, values)
        conn.close()
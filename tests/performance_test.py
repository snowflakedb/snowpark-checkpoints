from tests.src.utils.source_in.stress_input.Input_validators import Input_validators
from tests.src.utils.source_in.stress_input.Input_collectors import Input_collectors
from snowflake.snowpark import Session
from __version__ import __version__ as package_version   


def performance_test(package_name, file_name, size_file, execution_mode, limits, temp_path):
    """
    Executes a performance test for a given package and file, and logs the results in a Snowflake table.

    Parameters:
    package_name (str): The name of the package being tested (Collectors or Validators).
    file_name (str): The name of the file being tested.
    size_file (int): The input size of the file being tested.
    execution_mode (str): The mode in which the test is executed (Schema or Dataframe).
    limits (dict): A dictionary containing the memory and time limits for the test.
    temp_path (str): The temporary path for storing intermediate results.

    Raises:
    AssertionError: If the memory consumption or execution time exceeds the specified limits.
    """

    input_functions = {
            "Input_validators": Input_validators,
            "Input_collectors": Input_collectors,
        }
    errorMemory, errorTime = None, None
    memory, time = input_functions[(file_name)](execution_mode,temp_path )
    session = Session.builder.getOrCreate()
    df = session.sql("SELECT * FROM SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST").toPandas()
    filtered_df = df[
        (df["PACKAGE_NAME"] == package_name)
        & (df["SOURCE_IN"] == file_name)
        & (df["EXECUTION_MODE"] == execution_mode)
    ]

    if not filtered_df.empty:
        last_record = filtered_df.iloc[-1]
        errorMemory = (
            f"Error - Increased memory consumption in this version (Diference: {round((memory-last_record['MEMORY']),2)} MB)"
            if memory > last_record["MEMORY"]
            else None
        )
        errorTime = (
            f"Error - Increased time execution in this version (Diference: {round((time-last_record['TIME']),2)} seconds)"
            if time > last_record["TIME"]
            else None
        )
    sql = "INSERT INTO SNOWPARK_CHECKPOINTS_PERFORMANCE_TEST ( PACKAGE_NAME, PACKAGE_VERSION, SOURCE_IN ,SOURCE_SIZE, EXECUTION_MODE, MEMORY, TIME, ERRORMEMORY, ERRORTIME ) VALUES (?,?,?,?,?,?,?,?,?)"
    values = [
        package_name,
        package_version,
        file_name,
        size_file,
        execution_mode,
        memory,
        time,
        errorMemory,
        errorTime,
    ]
    session.sql(sql, params=values).collect()
    session.close()
    memory_within_limits= memory < limits[package_name]["sup_memory"]
    time_within_limits = time < limits[package_name]["sup_time"]

    if not memory_within_limits:
        assert (
            False
        ), f"Memory consumption {memory}MB is out of limit. Expected less than {limits[package_name]['sup_memory']}MB."

    if not time_within_limits:
        assert (
            False
        ), f"Execution time {time}seconds is out of limits. Expected less than {limits[package_name]['sup_time']}seconds."

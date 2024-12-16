import os

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLLECTION_RESULT_FILE_NAME,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
)


def get_output_file_path() -> str:
    """Get the output file path.

    Returns:
        str: returns the output file path.

    """
    current_working_directory_path = os.getcwd()
    output_directory_path = os.path.join(
        current_working_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    output_file_path = os.path.join(output_directory_path, COLLECTION_RESULT_FILE_NAME)
    return output_file_path


def get_relative_file_path(path: str) -> str:
    """Get the relative file path.

    Args:
        path (str): a file path.

    Returns:
        str: returns the relative file path of the given file.

    """
    relative_file_path = os.path.relpath(path)
    return relative_file_path


def get_output_directory_path() -> str:
    """Get the output directory path.

    Returns:
        str: returns the output directory path.

    """
    current_working_directory_path = os.getcwd()
    checkpoints_output_directory_path = os.path.join(
        current_working_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    return checkpoints_output_directory_path


def create_output_directory() -> None:
    """Create the output directory in the current working directory.

    The name of the directory is snowpark-checkpoints-output.

    """
    checkpoints_output_directory_path = get_output_directory_path()
    if not os.path.exists(checkpoints_output_directory_path):
        os.makedirs(checkpoints_output_directory_path)

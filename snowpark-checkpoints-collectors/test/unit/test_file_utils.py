import os.path
import tempfile
from unittest import mock

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLLECTION_RESULT_FILE_NAME,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
)
from snowflake.snowpark_checkpoints_collector.utils import file_utils


def test_get_output_file_path():
    output_file_path = file_utils.get_output_file_path()
    assert output_file_path.endswith(COLLECTION_RESULT_FILE_NAME) == True


def test_get_relative_file_path():
    file_name = os.path.basename(__file__)
    output_relative_file_path = file_utils.get_relative_file_path(__file__)
    assert output_relative_file_path.endswith(file_name) == True


def test_get_output_directory_path():
    output_directory_path = file_utils.get_output_directory_path()
    assert (
        output_directory_path.endswith(SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME)
        == True
    )


def test_create_output_directory():
    with mock.patch("os.makedirs") as os_make_dir:
        os_make_dir.side_effect = tempfile.mkdtemp()
        file_utils.create_output_directory()
        os_make_dir.assert_called()

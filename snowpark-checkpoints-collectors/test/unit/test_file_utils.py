# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os.path
import tempfile
from unittest import mock

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLLECTION_RESULT_FILE_NAME,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    UNKNOWN_SOURCE_FILE,
    UNKNOWN_LINE_OF_CODE,
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


def test_get_output_file_path_create():
    with mock.patch("os.makedirs") as os_make_dir_mock:
        os_make_dir_mock.side_effect = tempfile.mkdtemp()
        with mock.patch("os.path.exists") as path_exists_mock:
            path_exists_mock.return_value = False
            file_utils.get_output_file_path()
            os_make_dir_mock.assert_called()


def test_get_collection_point_source_file_path_scenario_python_source_file():
    collection_point_source_file_path = (
        file_utils.get_collection_point_source_file_path()
    )
    assert collection_point_source_file_path != UNKNOWN_SOURCE_FILE


def test_get_collection_point_source_file_path_scenario_notebook_source_file():
    with mock.patch(
        "snowflake.snowpark_checkpoints_collector.utils.file_utils._is_temporal_path",
        return_value=True,
    ):
        with mock.patch(
            "snowflake.snowpark_checkpoints_collector.utils.file_utils._get_ipynb_file_path_collection",
            return_value=["abc.ipynb"],
        ):
            collection_point_source_file_path = (
                file_utils.get_collection_point_source_file_path()
            )
            assert collection_point_source_file_path != UNKNOWN_SOURCE_FILE


def test_get_collection_point_source_file_path_scenario_unknown_source_file():
    with mock.patch(
        "snowflake.snowpark_checkpoints_collector.utils.file_utils._is_temporal_path",
        return_value=True,
    ):
        with mock.patch(
            "snowflake.snowpark_checkpoints_collector.utils.file_utils._get_ipynb_file_path_collection",
            return_value=["abc.ipynb", "def.ipynb"],
        ):
            collection_point_source_file_path = (
                file_utils.get_collection_point_source_file_path()
            )
            assert collection_point_source_file_path == UNKNOWN_SOURCE_FILE


def test_get_collection_point_source_file_path_scenario_exception():
    with mock.patch(
        "snowflake.snowpark_checkpoints_collector.utils.file_utils._is_temporal_path",
        side_effect=Exception("Mocked exception"),
    ):
        collection_point_source_file_path = (
            file_utils.get_collection_point_source_file_path()
        )
        assert collection_point_source_file_path == UNKNOWN_SOURCE_FILE


def test_get_collection_point_line_of_code_scenario_python_source_file():
    collection_point_line_of_code = file_utils.get_collection_point_line_of_code()
    assert collection_point_line_of_code != UNKNOWN_LINE_OF_CODE


def test_get_collection_point_line_of_code_scenario_notebook_source_file():
    with mock.patch(
        "snowflake.snowpark_checkpoints_collector.utils.file_utils._is_temporal_path",
        return_value=True,
    ):
        collection_point_line_of_code = file_utils.get_collection_point_line_of_code()
        assert collection_point_line_of_code == UNKNOWN_LINE_OF_CODE


def test_get_collection_point_line_of_code_scenario_exception():
    with mock.patch(
        "snowflake.snowpark_checkpoints_collector.utils.file_utils._is_temporal_path",
        side_effect=Exception("Mocked exception"),
    ):
        collection_point_line_of_code = file_utils.get_collection_point_line_of_code()
        assert collection_point_line_of_code == UNKNOWN_LINE_OF_CODE

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
import inspect
import os
import tempfile

from typing import Optional

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLLECTION_RESULT_FILE_NAME,
    DOT_IPYNB_EXTENSION,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    UNKNOWN_LINE_OF_CODE,
    UNKNOWN_SOURCE_FILE,
)


def get_output_file_path(out_path: Optional[str] = None) -> str:
    """Get the output file path.

    Args:
        out_path (Optional[str], optional): the output path. Defaults to None.

    Returns:
        str: returns the output file path.

    """
    output_directory_path = get_output_directory_path(out_path)
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


def get_output_directory_path(output_path: Optional[str] = None) -> str:
    """Get the output directory path.

    Returns:
        str: returns the output directory path.

    """
    current_working_directory_path = output_path if output_path else os.getcwd()
    checkpoints_output_directory_path = os.path.join(
        current_working_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    os.makedirs(checkpoints_output_directory_path, exist_ok=True)
    return checkpoints_output_directory_path


def get_collection_point_source_file_path() -> str:
    """Get the path of the source file where collection point it is.

    Returns:
        str: returns the path of source file where collection point it is.

    """
    try:
        collection_point_file_path = inspect.stack()[2].filename
        is_temporal_file_path = _is_temporal_path(collection_point_file_path)
        if is_temporal_file_path:
            ipynb_file_path_collection = _get_ipynb_file_path_collection()
            if len(ipynb_file_path_collection) == 1:
                collection_point_file_path = ipynb_file_path_collection[0]
            else:
                collection_point_file_path = UNKNOWN_SOURCE_FILE

        return collection_point_file_path

    except Exception:
        return UNKNOWN_SOURCE_FILE


def get_collection_point_line_of_code() -> int:
    """Find the line of code of the source file where collection point it is.

    Returns:
        int: returns the line of code of the source file where collection point it is.

    """
    try:
        collection_point_file_path = inspect.stack()[2].filename
        collection_point_line_of_code = inspect.stack()[2].lineno
        is_temporal_file_path = _is_temporal_path(collection_point_file_path)
        if is_temporal_file_path:
            collection_point_line_of_code = UNKNOWN_LINE_OF_CODE
        return collection_point_line_of_code

    except Exception:
        return UNKNOWN_LINE_OF_CODE


def _is_temporal_path(path: str) -> bool:
    temporal_directory_path = tempfile.gettempdir()
    is_temporal_path = path.startswith(temporal_directory_path)
    return is_temporal_path


def _get_ipynb_file_path_collection() -> list[str]:
    current_working_directory_path = os.getcwd()
    cwd_file_name_collection = os.listdir(current_working_directory_path)
    ipynb_file_path_collection = []
    for file_name in cwd_file_name_collection:
        is_ipynb_file = file_name.endswith(DOT_IPYNB_EXTENSION)
        if is_ipynb_file:
            file_path = os.path.join(current_working_directory_path, file_name)
            ipynb_file_path_collection.append(file_path)

    return ipynb_file_path_collection

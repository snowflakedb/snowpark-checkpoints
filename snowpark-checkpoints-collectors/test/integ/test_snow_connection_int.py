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

import os
import re

import pytest

from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)


def test_load_files_to_stage():

    snow_connection = SnowConnection()
    stage_id = snow_connection.stage_id
    stage_name = f"stage_name_test_{stage_id}"
    folder_name = "folder_name_test"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    input_directory_path = os.path.join(dir_path, "test_collect_df_mode_1_expected")

    filter_func = None
    snow_connection.create_tmp_stage(stage_name)
    snow_connection.load_files_to_stage(
        stage_name, folder_name, input_directory_path, filter_func
    )

    files = snow_connection.session.sql(f"LS @{stage_name}").collect()

    assert len(files) > 0


def test_load_files_to_stage_exception():

    snow_connection = SnowConnection()
    stage_id = snow_connection.stage_id
    stage_name = f"stage_name_test_{stage_id}"
    folder_name = "folder_name_test"
    dir_path = os.path.dirname(os.path.realpath(__file__))
    input_directory_path = os.path.join(dir_path, "test_collect_df_mode_1_expected")

    def none_file_filter_func(file: str):
        return False

    snow_connection.create_tmp_stage(stage_name)
    expected_msg = f"No files were found in the input directory: {input_directory_path}"
    with pytest.raises(Exception) as ex_info:
        snow_connection.load_files_to_stage(
            stage_name, folder_name, input_directory_path, none_file_filter_func
        )
    assert expected_msg == str(ex_info.value)

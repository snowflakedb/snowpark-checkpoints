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
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, call

import pytest

from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)


@pytest.mark.parametrize(
    "input_path",
    [
        "output_directory_path_test",
        "C:\\dir\\output_directory_path_test",
        "/dir/output_directory_path_test",
    ],
)
def test_create_snowflake_table_from_parquet(input_path):
    mocked_session = MagicMock()
    mock_df = MagicMock()
    mock_read = MagicMock()
    mock_save_as_table = MagicMock()

    mocked_session.sql.return_value = mock_df
    mock_df.collect.return_value = ["row1", "row2"]

    mocked_session.read = mock_read
    mock_read.parquet.return_value = mock_df

    mock_df.write = mock_save_as_table
    mock_save_as_table.save_as_table.return_value = ["row1", "row2"]

    output_directory_path_full = os.path.abspath(input_path)
    parquet_file_path = f"{input_path}/dir1/file1.parquet"
    stage_name = "CHECKPOINT_STAGE_{}"
    checkpoint_name = "checkpoint_name_test"

    with mock.patch("glob.glob") as glob_mock:
        with mock.patch("os.path.isfile") as isfile_mock:
            isfile_mock.return_value = True
            glob_mock.return_value = [parquet_file_path]
            snow_connection = SnowConnection(mocked_session)
            snow_connection.create_snowflake_table_from_local_parquet(
                checkpoint_name, input_path, stage_path=checkpoint_name
            )

    stage_name = stage_name.format(snow_connection.stage_id)

    assert mocked_session.method_calls[0] == call.sql(
        f"CREATE TEMP STAGE IF NOT EXISTS {stage_name}"
    )

    expected_file = Path(output_directory_path_full) / "dir1" / "file1.parquet"
    expected_file = Path(expected_file.as_posix()).as_uri()
    assert mocked_session.method_calls[1] == call.sql(
        f"PUT '{expected_file}' "
        f"'@{stage_name}/{checkpoint_name}/dir1/file1.parquet' "
        "AUTO_COMPRESS=FALSE"
    )

    assert mocked_session.method_calls[2] == call.sql(
        f"LIST '@{stage_name}/{checkpoint_name}'"
    )

    assert mocked_session.method_calls[3] == call.read.parquet(
        path=f"'@{stage_name}/{checkpoint_name}'"
    )

    assert mock_df.method_calls[3] == call.write.save_as_table(
        table_name="checkpoint_name_test", mode="overwrite"
    )

    assert mocked_session.method_calls[4] == call.sql(
        f"REMOVE '@{stage_name}/{checkpoint_name}'"
    )


def test_create_snowflake_table_from_parquet_exception():
    mocked_session = MagicMock()
    mocked_session.sql.side_effect = Exception("Test exception")
    checkpoint_name = "checkpoint_name_test"
    output_directory_path = "output_directory_path_test"

    with pytest.raises(Exception) as ex_info:
        snow_connection = SnowConnection(mocked_session)
        snow_connection.create_snowflake_table_from_local_parquet(
            checkpoint_name, output_directory_path
        )
    assert "Test exception" == str(ex_info.value)

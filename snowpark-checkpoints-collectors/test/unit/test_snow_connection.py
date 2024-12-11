#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock
from unittest.mock import MagicMock, call

from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)


def test_upload_to_snowflake():
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

    parquet_file_name = "file1.parquet"
    parquet_file_path = "dir1/file1.parquet"
    mock_dir_entry = type(
        "", (object,), {"name": parquet_file_name, "path": parquet_file_path}
    )()
    checkpoint_name = "checkpoint_name_test"
    checkpoint_file_name = "checkpoint_file_name_test.parquet"
    output_directory_path = "output_directory_path_test"

    with mock.patch("os.scandir") as os_mock:
        os_mock.return_value = [mock_dir_entry]
        snow_connection = SnowConnection(mocked_session)
        snow_connection.upload_to_snowflake(
            checkpoint_name, checkpoint_file_name, output_directory_path
        )

    assert mocked_session.method_calls[0] == call.sql(
        "CREATE TEMPORARY STAGE IF NOT EXISTS CHECKPOINT_STAGE"
    )

    assert mocked_session.method_calls[1] == call.sql(
        "PUT 'file://dir1/file1.parquet' "
        "'@CHECKPOINT_STAGE/checkpoint_file_name_test.parquet' "
        "AUTO_COMPRESS=FALSE"
    )

    assert mocked_session.method_calls[2] == call.read.parquet(
        path="'@CHECKPOINT_STAGE/checkpoint_file_name_test.parquet'"
    )

    assert mock_df.method_calls[2] == call.write.save_as_table(
        table_name="checkpoint_name_test", mode="overwrite"
    )

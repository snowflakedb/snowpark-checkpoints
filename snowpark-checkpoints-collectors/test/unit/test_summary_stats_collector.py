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

import logging
import os
import tempfile

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from snowflake.snowpark_checkpoints_collector.summary_stats_collector import (
    collect_dataframe_checkpoint,
    generate_parquet_for_spark_df,
)


def test_generate_parquet_for_spark_df_exception():
    spark = MagicMock()
    spark_df = MagicMock()
    spark_df.dtypes = []
    spark_df.select = MagicMock()
    spark_df = spark.createDataFrame()
    parquet_directory = os.path.join(
        tempfile.gettempdir(),
        f"test_spark_df_checkpoint_{datetime.now().strftime('%Y%m%d%H%M%S')}",
    )

    with pytest.raises(Exception, match="No parquet files were generated."):
        generate_parquet_for_spark_df(spark_df, parquet_directory)


def test_collect_dataframe_checkpoint_disabled_checkpoint(
    caplog: pytest.LogCaptureFixture,
):
    """Test that collect_dataframe_checkpoint logs a message when the checkpoint is disabled."""
    pyspark_df = MagicMock()
    checkpoint_name = "my_checkpoint"
    module_name = "snowflake.snowpark_checkpoints_collector.summary_stats_collector"
    expected_log_msg = (
        f"Checkpoint '{checkpoint_name}' is disabled. Skipping collection."
    )

    with (
        caplog.at_level(
            level=logging.INFO,
            logger=module_name,
        ),
        patch(
            f"{module_name}.is_checkpoint_enabled",
            return_value=False,
        ) as mock_is_checkpoint_enabled,
    ):
        collect_dataframe_checkpoint(pyspark_df, checkpoint_name)

    mock_is_checkpoint_enabled.assert_called_once_with(checkpoint_name)
    assert expected_log_msg in caplog.messages

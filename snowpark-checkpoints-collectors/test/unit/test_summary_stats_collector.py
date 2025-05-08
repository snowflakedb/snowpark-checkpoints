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
from typing import get_type_hints
from unittest.mock import MagicMock, patch

import pytest

from snowflake.snowpark_checkpoints_collector.summary_stats_collector import (
    collect_dataframe_checkpoint,
    generate_parquet_for_spark_df,
    xcollect_dataframe_checkpoint,
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
    expected_exception_error_msg = "Checkpoint 'my_checkpoint' is disabled. Please enable it in the checkpoints.json file."
    expected_fix_suggestion_msg = "In case you want to skip it, use the xcollect_dataframe_checkpoint method instead."
    try:
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
    except Exception as e:
        mock_is_checkpoint_enabled.assert_called_once_with(checkpoint_name)
        error_msg = e.args[0]
        fix_suggestion_msg = e.args[1]
        assert error_msg == expected_exception_error_msg
        assert fix_suggestion_msg == expected_fix_suggestion_msg


def test_skip_collector_parameters_commutability():
    collect_hints = get_type_hints(collect_dataframe_checkpoint)
    x_collect_hints = get_type_hints(xcollect_dataframe_checkpoint)

    collect_params = {
        name: hint for name, hint in collect_hints.items() if name != "return"
    }
    x_collect_params = {
        name: hint for name, hint in x_collect_hints.items() if name != "return"
    }
    assert (
        collect_params == x_collect_params
    ), "The parameters of collect_dataframe_checkpoint and xcollect_dataframe_checkpoint must be the same."


def test_skip_collector_return_type_commutability():
    collect_hints = get_type_hints(collect_dataframe_checkpoint)
    x_collect_hints = get_type_hints(xcollect_dataframe_checkpoint)

    collect_return = collect_hints.get("return")
    x_collect_return = x_collect_hints.get("return")
    assert (
        collect_return == x_collect_return
    ), "The return type of collect_dataframe_checkpoint and xcollect_dataframe_checkpoint must be the same."

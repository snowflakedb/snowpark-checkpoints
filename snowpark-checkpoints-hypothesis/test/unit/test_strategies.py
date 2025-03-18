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
import re

from unittest.mock import MagicMock, Mock, patch

import pytest

from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.hypothesis_snowpark.constants import CUSTOM_DATA_KEY, PANDERA_SCHEMA_KEY
from snowflake.hypothesis_snowpark.strategies import (
    _dataframe_strategy_from_json_schema,
    _dataframe_strategy_from_object_schema,
)
from snowflake.snowpark import Session


LOGGER_NAME = "snowflake.hypothesis_snowpark.strategies"


def test_dataframe_strategy_none_session(caplog: pytest.LogCaptureFixture):
    expected_error_msg = "Session cannot be None."
    with pytest.raises(ValueError, match=expected_error_msg), caplog.at_level(
        level=logging.ERROR, logger=LOGGER_NAME
    ):
        dataframe_strategy(schema="schema.json", session=None)
    assert expected_error_msg in caplog.text


def test_dataframe_strategy_invalid_schema(caplog: pytest.LogCaptureFixture):
    expected_error_msg = "Schema must be a path to a JSON schema file or a Pandera DataFrameSchema object."
    with pytest.raises(ValueError, match=expected_error_msg), caplog.at_level(
        level=logging.ERROR, logger=LOGGER_NAME
    ):
        dataframe_strategy(schema="123", session=Mock(spec=Session))
    assert expected_error_msg in caplog.text


def test_dataframe_strategy_invalid_json_file(caplog: pytest.LogCaptureFixture):
    mock_json_schema = {
        "key1": "value1",
        "key2": "value2",
    }
    expected_error_msg = "Invalid JSON schema. The JSON schema must contain 'pandera_schema' and 'custom_data' keys."

    with patch(
        "snowflake.hypothesis_snowpark.strategies.load_json_schema",
        return_value=mock_json_schema,
    ), pytest.raises(ValueError, match=expected_error_msg,), caplog.at_level(
        level=logging.ERROR, logger=LOGGER_NAME
    ):
        dataframe_strategy(
            schema="schema.json",
            session=Mock(spec=Session),
        )
    assert expected_error_msg in caplog.text


def test_dataframe_strategy_not_supported_dtypes(caplog: pytest.LogCaptureFixture):
    mock_json_schema = {
        "pandera_schema": {
            "columns": {
                "daytimeinterval_column": {"dtype": "timedelta64[ns]"},
                "map_column": {"dtype": "object"},
                "void_column": {"dtype": "object"},
                "struct_column": {"dtype": "object"},
            },
        },
        "custom_data": {
            "columns": [
                {"name": "daytimeinterval_column", "type": "daytimeinterval"},
                {"name": "map_column", "type": "map"},
                {"name": "void_column", "type": "void"},
                {"name": "struct_column", "type": "struct"},
            ]
        },
    }
    expected_error_msg = (
        "The following data types are not supported by the Snowpark DataFrame strategy: "
        "['daytimeinterval', 'map', 'void', 'struct']"
    )

    with patch(
        "snowflake.hypothesis_snowpark.strategies.load_json_schema",
        return_value=mock_json_schema,
    ), pytest.raises(ValueError, match=re.escape(expected_error_msg),), caplog.at_level(
        level=logging.ERROR, logger=LOGGER_NAME
    ):
        dataframe_strategy(
            schema="schema.json",
            session=Mock(spec=Session),
        )
    assert expected_error_msg in caplog.text


def test_object_schema_inner_dataframe_strategy_logs_exceptions(
    caplog: pytest.LogCaptureFixture,
):
    """Test that the inner _dataframe_strategy function logs exceptions when called with a Pandera schema object."""
    schema = MagicMock()
    exception_message = "Test exception"

    schema.strategy.side_effect = ValueError(exception_message)
    strategy = _dataframe_strategy_from_object_schema(
        schema=schema, session=MagicMock()
    )

    with pytest.raises(ValueError, match=exception_message), caplog.at_level(
        level=logging.ERROR, logger=LOGGER_NAME
    ):
        strategy.example()

    assert "An error occurred in _dataframe_strategy" in caplog.text
    assert exception_message in caplog.text


@patch("snowflake.hypothesis_snowpark.strategies.load_json_schema")
@patch("snowflake.hypothesis_snowpark.strategies.update_pandas_df_strategy")
def test_json_schema_inner_dataframe_strategy_logs_exceptions(
    mock_update_pandas_df_strategy: Mock,
    mock_load_json_schema: Mock,
    caplog: pytest.LogCaptureFixture,
):
    """Test that the inner _dataframe_strategy function logs exceptions when called with a JSON schema file."""
    json_schema_content = {
        PANDERA_SCHEMA_KEY: {"columns": {}},
        CUSTOM_DATA_KEY: {"columns": []},
    }
    exception_message = "Test exception"

    mock_load_json_schema.return_value = json_schema_content
    mock_update_pandas_df_strategy.side_effect = ValueError(exception_message)
    strategy = _dataframe_strategy_from_json_schema(
        schema="dummy.json", session=MagicMock()
    )

    with pytest.raises(ValueError, match=exception_message), caplog.at_level(
        level=logging.ERROR, logger=LOGGER_NAME
    ):
        strategy.example()

    assert "An error occurred in _dataframe_strategy" in caplog.text
    assert exception_message in caplog.text

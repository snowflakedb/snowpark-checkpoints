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

import re

from unittest.mock import Mock, patch

import pytest

from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.snowpark import Session


def test_dataframe_strategy_none_session():
    with pytest.raises(ValueError, match="Session cannot be None."):
        dataframe_strategy(schema="schema.json", session=None)


def test_dataframe_strategy_invalid_schema():
    with pytest.raises(
        ValueError,
        match="Schema must be a path to a JSON schema file or a Pandera DataFrameSchema object.",
    ):
        dataframe_strategy(schema="123", session=Mock(spec=Session))


def test_dataframe_strategy_invalid_json_file():
    mock_json_schema = {
        "key1": "value1",
        "key2": "value2",
    }

    with patch(
        "snowflake.hypothesis_snowpark.strategies.load_json_schema",
        return_value=mock_json_schema,
    ):
        with pytest.raises(
            ValueError,
            match=(
                "Invalid JSON schema. "
                "The JSON schema must contain 'pandera_schema' and 'custom_data' keys."
            ),
        ):
            dataframe_strategy(
                schema="schema.json",
                session=Mock(spec=Session),
            )


def test_dataframe_strategy_not_supported_dtypes():
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

    with patch(
        "snowflake.hypothesis_snowpark.strategies.load_json_schema",
        return_value=mock_json_schema,
    ):
        with pytest.raises(
            ValueError,
            match=re.escape(
                "The following data types are not supported by the Snowpark DataFrame strategy: "
                "['daytimeinterval', 'map', 'void', 'struct']"
            ),
        ):
            dataframe_strategy(
                schema="schema.json",
                session=Mock(spec=Session),
            )

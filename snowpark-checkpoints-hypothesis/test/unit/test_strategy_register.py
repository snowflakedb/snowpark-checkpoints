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

import pytest

from snowflake.hypothesis_snowpark.strategy_register import (
    register_strategy,
    snowpark_strategies,
)


LOGGER_NAME = "snowflake.hypothesis_snowpark.strategy_register"


def test_register_strategy_registers_a_function(caplog: pytest.LogCaptureFixture):
    dtype = "int"

    with caplog.at_level(level=logging.DEBUG, logger=LOGGER_NAME):

        @register_strategy(dtype)
        def strategy_int():
            return "int strategy"

    assert strategy_int() == "int strategy"
    assert dtype in snowpark_strategies
    assert snowpark_strategies[dtype]() == "int strategy"
    assert strategy_int.__name__ in caplog.text
    assert dtype in caplog.text


def test_register_strategy_overwrite_existing_strategy(
    caplog: pytest.LogCaptureFixture,
):
    dtype = "int"

    with caplog.at_level(level=logging.DEBUG, logger=LOGGER_NAME):

        @register_strategy(dtype)
        def strategy_int():
            return "int strategy"

        @register_strategy(dtype)
        def new_strategy_int():
            return "new int strategy"

    assert strategy_int() == "int strategy"
    assert new_strategy_int() == "new int strategy"
    assert dtype in snowpark_strategies
    assert snowpark_strategies[dtype]() == "new int strategy"
    for substring in [
        str(strategy_int.__name__),
        str(new_strategy_int.__name__),
        str(dtype),
    ]:
        assert substring in caplog.text

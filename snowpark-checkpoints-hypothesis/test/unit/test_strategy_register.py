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

from snowflake.hypothesis_snowpark.strategy_register import (
    register_strategy,
    snowpark_strategies,
)


def test_register_strategy_registers_a_function():
    dtype = "int"

    @register_strategy(dtype)
    def strategy_int():
        return "int strategy"

    assert strategy_int() == "int strategy"
    assert dtype in snowpark_strategies
    assert snowpark_strategies[dtype]() == "int strategy"


def test_register_strategy_overwrite_existing_strategy():
    dtype = "int"

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

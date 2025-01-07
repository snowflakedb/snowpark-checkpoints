#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

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

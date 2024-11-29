#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from hypothesis.strategies import composite


snowpark_strategies = {}


def register(*args):
    """Register a function into the snowpark_strategies dictionary and compose it as a SearchStrategy.

    Args:
        *args: Type or key to be used for registering the strategy in the snowpark_strategies dictionary.
               If no arguments are provided, the function is returned as-is.

    Returns:
        Callable: A wrapper function that either:
            - Registers a composite strategy in snowpark_strategies (if args are provided)
            - Returns the original function (if no args are provided)

    """

    def wrapper(func):
        if len(args) > 0:
            composed_func = composite(func)
            snowpark_strategies[args[0]] = composed_func()
        else:
            composed_func = func
        return composed_func

    return wrapper

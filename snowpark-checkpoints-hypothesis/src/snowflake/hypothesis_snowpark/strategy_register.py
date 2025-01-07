#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


from functools import wraps
from typing import Callable


snowpark_strategies: dict[str, Callable] = {}


def register_strategy(dtype: str) -> Callable:
    """Register a strategy for a given data type.

    Args:
        dtype: The data type to register the strategy for.

    Returns:
        The decorated function.

    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        snowpark_strategies[dtype] = func
        return wrapper

    return decorator

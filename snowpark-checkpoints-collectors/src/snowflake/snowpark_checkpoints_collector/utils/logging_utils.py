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

from functools import wraps
from typing import Callable, Optional, TypeVar

from typing_extensions import ParamSpec


P = ParamSpec("P")
R = TypeVar("R")


def log(
    _func: Optional[Callable[P, R]] = None,
    *,
    logger: Optional[logging.Logger] = None,
    log_args: bool = True,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Log the function call and any exceptions that occur.

    Args:
        _func: The function to log.
        logger: The logger to use for logging. If not provided, a logger will be created using the
                function's module name.
        log_args: Whether to log the arguments passed to the function.

    Returns:
        A decorator that logs the function call and any exceptions that occur.

    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            _logger = logging.getLogger(func.__module__) if logger is None else logger
            if log_args:
                args_repr = [repr(a) for a in args]
                kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
                formatted_args = ", ".join([*args_repr, *kwargs_repr])
                _logger.debug("%s called with args %s", func.__name__, formatted_args)
            try:
                return func(*args, **kwargs)
            except Exception:
                _logger.exception("An error occurred in %s", func.__name__)
                raise

        return wrapper

    # Handle the case where the decorator is used without parentheses
    if _func is None:
        return decorator
    return decorator(_func)

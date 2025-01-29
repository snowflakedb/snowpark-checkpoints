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

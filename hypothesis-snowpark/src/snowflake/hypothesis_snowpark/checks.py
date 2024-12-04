#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from datetime import date, timedelta
from typing import Optional

import hypothesis.strategies as st
import pandera as pa

from pandas import Series
from pandera import extensions


def date_strategy(
    pandera_dtype: pa.DataType,
    strategy: Optional[st.SearchStrategy] = None,
    *,
    min_value=date.min,
    max_value=date.max,
    include_min=True,
    include_max=True,
):
    """Create a Hypothesis strategy for generating dates within a specified range.

    Args:
        pandera_dtype: The Pandera data type.
        strategy: The chained strategy to use.
        min_value: The minimum date value.
        max_value: The maximum date value.
        include_min: Whether to include the minimum date value.
        include_max: Whether to include the maximum date value.

    Returns:
        A Hypothesis strategy that generates dates within the specified range.

    """
    if strategy is not None:
        raise ValueError("Chaining strategies are not supported for date dtype.")

    if not include_min:
        min_value = min_value + timedelta(days=1)
    if not include_max:
        max_value = max_value - timedelta(days=1)

    if min_value > max_value:
        raise ValueError(
            "Invalid range: min_value must be less than or equal to max_value."
        )

    return st.dates(min_value=min_value, max_value=max_value)


@extensions.register_check_method(
    statistics=["min_value", "max_value", "include_min", "include_max"],
    strategy=date_strategy,
)
def dates_in_range(
    pandas_obj: Series,
    *,
    min_value: date = date.min,
    max_value: date = date.max,
    include_min: bool = True,
    include_max: bool = True,
) -> bool:
    """Check if a date is within the specified range.

    Args:
        pandas_obj: The pandas Series object to check.
        min_value: The minimum date value.
        max_value: The maximum date value.
        include_min: Whether to include the minimum date value.
        include_max: Whether to include the maximum date value.

    Returns:
        True if the date is within the specified range, False otherwise.

    """
    actual_date = pandas_obj.dt.date
    is_in_range = (
        actual_date >= min_value if include_min else actual_date > min_value
    ) & (actual_date <= max_value if include_max else actual_date < max_value)
    return is_in_range

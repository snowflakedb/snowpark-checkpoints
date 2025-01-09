#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt

from decimal import Decimal
from fractions import Fraction
from typing import Optional, Union

import hypothesis.strategies as st
import pandas as pd

from snowflake.hypothesis_snowpark.constants import (
    CUSTOM_DATA_MAX_SIZE_KEY,
    CUSTOM_DATA_MIN_SIZE_KEY,
    CUSTOM_DATA_NAME_KEY,
    CUSTOM_DATA_TYPE_KEY,
    CUSTOM_DATA_VALUE_TYPE_KEY,
    PYSPARK_ARRAY_TYPE,
    PYSPARK_BINARY_TYPE,
    PYSPARK_BOOLEAN_TYPE,
    PYSPARK_BYTE_TYPE,
    PYSPARK_DATE_TYPE,
    PYSPARK_DECIMAL_TYPE,
    PYSPARK_DOUBLE_TYPE,
    PYSPARK_FLOAT_TYPE,
    PYSPARK_INTEGER_TYPE,
    PYSPARK_LONG_TYPE,
    PYSPARK_SHORT_TYPE,
    PYSPARK_STRING_TYPE,
)
from snowflake.hypothesis_snowpark.strategy_register import (
    register_strategy,
    snowpark_strategies,
)


@register_strategy(PYSPARK_ARRAY_TYPE)
@st.composite
def array_strategy(
    draw: st.DrawFn, dtype: str, min_size: int = 0, max_size: Optional[int] = None
) -> list:
    """Generate a list of values for a given data type.

    Args:
        draw: The Hypothesis draw function.
        dtype: The data type of the array.
        min_size: The minimum size of the array.
        max_size: The maximum size of the array.

    Returns:
        A list of values for the given data type.

    """
    if dtype not in snowpark_strategies:
        raise ValueError(f"Not implemented SearchStrategy for arrays of type '{dtype}'")

    strategy = snowpark_strategies[dtype]
    return draw(st.lists(strategy(), min_size=min_size, max_size=max_size))


@register_strategy(PYSPARK_BINARY_TYPE)
@st.composite
def binary_strategy(
    draw: st.DrawFn, min_size: int = 0, max_size: Optional[int] = None
) -> bytes:
    """Generate a binary data (bytes) with a given length.

    Args:
        draw: The Hypothesis draw function.
        min_size: The minimum length of the binary data.
        max_size: The maximum length of the binary data.

    Returns:
        A binary data (bytes) with a given length.

    """
    return draw(st.binary(min_size=min_size, max_size=max_size))


@register_strategy(PYSPARK_BOOLEAN_TYPE)
@st.composite
def boolean_strategy(draw: st.DrawFn) -> bool:
    """Generate a boolean value.

    Args:
        draw: The Hypothesis draw function.

    Returns:
        A boolean value.

    """
    return draw(st.booleans())


@register_strategy(PYSPARK_BYTE_TYPE)
@st.composite
def byte_strategy(draw: st.DrawFn) -> int:
    """Generate a byte value.

    Args:
        draw: The Hypothesis draw function.

    Returns:
        A byte value.

    """
    return draw(st.integers(min_value=-128, max_value=127))


@register_strategy(PYSPARK_DATE_TYPE)
@st.composite
def date_strategy(
    draw: st.DrawFn, min_value: dt.date = dt.date.min, max_value: dt.date = dt.date.max
) -> dt.date:
    """Generate a date value within a given range.

    Args:
        draw: The Hypothesis draw function.
        min_value: The minimum date value.
        max_value: The maximum date value.

    Returns:
        A date value within a given range.

    """
    return draw(st.dates(min_value=min_value, max_value=max_value))


@register_strategy(PYSPARK_DECIMAL_TYPE)
@st.composite
def decimal_strategy(
    draw: st.DrawFn,
    min_value: Union[int, float, Fraction, Decimal, str, None] = None,
    max_value: Union[int, float, Fraction, Decimal, str, None] = None,
) -> Decimal:
    """Generate a decimal value within a given range.

    Args:
        draw: The Hypothesis draw function.
        min_value: The minimum value of the decimal.
        max_value: The maximum value of the decimal.

    Returns:
        A decimal value within a given range.

    """
    return draw(st.decimals(min_value=min_value, max_value=max_value))


@register_strategy(PYSPARK_FLOAT_TYPE)
@register_strategy(PYSPARK_DOUBLE_TYPE)
@st.composite
def float_strategy(
    draw: st.DrawFn,
    min_value: Union[int, float, Fraction, Decimal, None] = None,
    max_value: Union[int, float, Fraction, Decimal, None] = None,
) -> float:
    """Generate a float value within a given range.

    Args:
        draw: The Hypothesis draw function.
        min_value: The minimum value of the float.
        max_value: The maximum value of the float.

    Returns:
        A float value within a given range.

    """
    return draw(st.floats(min_value=min_value, max_value=max_value))


@register_strategy(PYSPARK_INTEGER_TYPE)
@st.composite
def integer_strategy(
    draw: st.DrawFn,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    """Generate an integer value within a given range.

    Args:
        draw: The Hypothesis draw function.
        min_value: The minimum value of the integer.
        max_value: The maximum value of the integer.

    Returns:
        An integer value within a given range.

    """
    return draw(st.integers(min_value=min_value, max_value=max_value))


@register_strategy(PYSPARK_LONG_TYPE)
@st.composite
def long_strategy(draw: st.DrawFn) -> int:
    """Generate a long value.

    Args:
        draw: The Hypothesis draw function.

    Returns:
        A long value.

    """
    return draw(st.integers(min_value=-(2**63), max_value=2**63 - 1))


@register_strategy(PYSPARK_SHORT_TYPE)
@st.composite
def short_strategy(draw: st.DrawFn) -> int:
    """Generate a short value.

    Args:
        draw: The Hypothesis draw function.

    Returns:
        A short value.

    """
    return draw(st.integers(min_value=-(2**15), max_value=2**15 - 1))


@register_strategy(PYSPARK_STRING_TYPE)
@st.composite
def string_strategy(
    draw: st.DrawFn, min_size: int = 0, max_size: Optional[int] = None
) -> str:
    """Generate a string value with a given length.

    Args:
        draw: The Hypothesis draw function.
        min_size: The minimum length of the string.
        max_size: The maximum length of the string.

    Returns:
        A string value with a given length

    """
    return draw(st.text(min_size=min_size, max_size=max_size))


@st.composite
def update_pandas_df_strategy(
    draw: st.DrawFn,
    pandas_df: pd.DataFrame,
    columns: list[dict],
):
    """Apply custom strategies to a Pandas DataFrame based on the custom data.

    Args:
        draw: The Hypothesis draw function.
        pandas_df: The Pandas DataFrame to apply custom strategies to.
        columns: The custom data columns to apply strategies to.

    Returns:
        A Pandas DataFrame with custom strategies applied.

    """
    pandas_df_copy = pandas_df.copy()
    number_of_rows = len(pandas_df_copy)

    for column in columns:
        column_dtype = column.get(CUSTOM_DATA_TYPE_KEY)
        strategy = snowpark_strategies.get(column_dtype, None)

        if strategy is None:
            raise ValueError(
                f"Unsupported custom strategy for data type: {column_dtype}"
            )

        strategy_kwargs = {}

        if column_dtype == PYSPARK_ARRAY_TYPE:
            strategy_kwargs = {
                "dtype": column.get(CUSTOM_DATA_VALUE_TYPE_KEY),
                "min_size": column.get(CUSTOM_DATA_MIN_SIZE_KEY, 0),
                "max_size": column.get(CUSTOM_DATA_MAX_SIZE_KEY, None),
            }
        elif column_dtype == PYSPARK_BINARY_TYPE:
            strategy_kwargs = {
                "min_size": column.get(CUSTOM_DATA_MIN_SIZE_KEY, 0),
                "max_size": column.get(CUSTOM_DATA_MAX_SIZE_KEY, None),
            }

        column_name = column.get(CUSTOM_DATA_NAME_KEY)
        pandas_df_copy[column_name] = draw(
            st.lists(
                strategy(**strategy_kwargs),
                min_size=number_of_rows,
                max_size=number_of_rows,
            )
        )

    return pandas_df_copy

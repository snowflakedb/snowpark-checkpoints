#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Sequence
from typing import Callable, Optional, TypeVar

from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy

import snowflake.snowpark

from snowflake.hypothesis_snowpark.strategy_register import register
from snowflake.hypothesis_snowpark.strategy_register import (
    snowpark_strategies as snowpark_st,
)
from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


T = TypeVar("T")


@register(BooleanType)
def boolean(draw):
    """Generate a boolean value using Hypothesis strategy.

    Args:
        draw (Callable): Hypothesis draw function for generating values.

    Returns:
        bool: A randomly generated boolean value.

    """
    return draw(st.booleans())


@register(IntegerType)
def integers(draw):
    """Generate an integer value using Hypothesis strategy.

    Args:
        draw (Callable): Hypothesis draw function for generating values.

    Returns:
        int: A randomly generated integer value.

    """
    return draw(st.integers())


@register(StringType)
def text(draw):
    """Generate a text string using Hypothesis strategy.

    Args:
        draw (Callable): Hypothesis draw function for generating values.

    Returns:
        str: A randomly generated text string.

    """
    return draw(st.text())


@st.composite
def snowpark_dataframe(
    draw: Callable[[SearchStrategy], T],
    *,
    columns: Sequence[StructField],
    min_rows: int = 5,
    max_rows: int = 20,
    session: Optional[snowflake.snowpark.Session] = None,
):
    """Define a Hypothesis strategy to generate Snowpark DataFrames.

    Args:
        draw (Callable[[SearchStrategy], T]): A function that allows generating values from other strategies.
        columns (Sequence[StructField]): A sequence of Snowpark StructFields defining column schema.
        min_rows (int, optional): Minimum number of rows in the generated DataFrame.
             Defaults to 5.
        max_rows (int, optional): Maximum number of rows in the generated DataFrame.
            Defaults to 20.
        session (Optional[snowflake.snowpark.Session], optional): Snowpark session to create the DataFrame.
            Defaults to None.

    Raises:
        Exception: If a strategy is not implemented for a specific column type.

    Returns:
        snowflake.snowpark.DataFrame: A Snowpark DataFrame with randomly generated data.

    """
    candidate_st = []
    for col in columns:
        if type(col.datatype) in snowpark_st:
            col_strategy = snowpark_st[type(col.datatype)]
            candidate_st.append(col_strategy)
        else:
            raise Exception(
                f"Not Implemented SearchStrategy for column {col.name} of type {col.datatype}"
            )

    data = draw(
        st.lists((st.tuples(*candidate_st)), min_size=min_rows, max_size=max_rows)
    )

    session = (
        Session.builder.config("local_testing", True).create()
        if session is None
        else session
    )

    df = session.create_dataframe(data, schema=StructType([*columns]))

    return df

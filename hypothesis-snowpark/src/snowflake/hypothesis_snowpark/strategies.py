from typing import Callable, TypeVar, Optional, Sequence

import snowflake.snowpark
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, BooleanType

from snowflake.hypothesis_snowpark.strategy_register import register, snowpark_strategies as snowpark_st

T = TypeVar("T")


@register(BooleanType)
def boolean(draw):
    return draw(st.booleans())


@register(IntegerType)
def integers(draw):
    return draw(st.integers())


@register(StringType)
def text(draw):
    return draw(st.text())


@st.composite
def snowpark_dataframe(
        draw: Callable[[SearchStrategy], T],
        *,
        columns: Sequence[StructField],
        min_rows: int = 5,
        max_rows: int = 20,
        session: Optional[snowflake.snowpark.Session] = None
) -> DataFrame:
    """
    Defines a hypothesis strategy to generate Snowpark :class:`snowflake.snowpark.DataFrame`.




    :param draw: Special function that allows to generate values from other strategies within this custom strategy.
    :param columns: required parameter a Sequence of Snowpark StructFields with the column definition.
    :param min_rows: minimum number of rows in the data frame to be drawn. Default value is 5 rows.
    :param max_rows: maximum number of rows in the data frame to be drawn. Default value is 20 rows.
    :param session: Session to be used to create the dataframe, if no provided it will use a "local_testing" session.
    :return: a Snowpark DataFrame.
    """

    # Data type that we should support: integers, booleans, strings, bytes, null
    # Proposed method signature:
    # def snowpark_dataframe(
    #     draw: Callable[[SearchStrategy], T],
    #     columns: List[StructField],
    #     session: Session = None,
    # ): ...

    candidate_st = []
    for col in columns:
        if type(col.datatype) in snowpark_st:
            col_strategy = snowpark_st[type(col.datatype)]
            print(type(col_strategy))
            candidate_st.append(col_strategy)
        else:
            raise Exception(f'Not Implemented SearchStrategy for column {col.name} of type {col.datatype}')
    print(candidate_st)
    data = draw(st.lists((st.tuples(*candidate_st)), min_size=min_rows, max_size=max_rows))

    session = Session.builder.config("local_testing", True).create() if session is None else session

    df = session.create_dataframe(data, schema=StructType([*columns]))

    return df

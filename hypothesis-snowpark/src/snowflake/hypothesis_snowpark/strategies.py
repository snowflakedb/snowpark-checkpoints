from typing import Callable, TypeVar
from hypothesis import strategies as st
from hypothesis.strategies import SearchStrategy
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType

T = TypeVar("T")


@st.composite
def snowpark_dataframe(
    draw: Callable[[SearchStrategy], T],
) -> SearchStrategy[DataFrame]:
    """
    Defines a hypothesis strategy to generate Snowpark :class:`snowflake.snowpark.DataFrame`.

    :param draw: Special function that allows to generate values from other strategies within this custom strategy.
    :return: a Snowpark DataFrame.
    """

    # Data type that we should support: integers, booleans, strings, bytes, null
    # Proposed method signature:
    # def snowpark_dataframe(
    #     draw: Callable[[SearchStrategy], T],
    #     columns: Optional[Sequence[StructField]],
    #     session: Session = None,
    # ): ...

    schema = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=False),
        ]
    )

    ids = draw(
        st.lists(st.integers(min_value=1, max_value=100), min_size=5, max_size=5)
    )
    names = draw(st.lists(st.text(min_size=1, max_size=10), min_size=5, max_size=5))

    session = Session.builder.config("local_testing", True).create()
    data = [(i, n) for i, n in zip(ids, names)]
    df = session.create_dataframe(data, schema=schema)

    return df

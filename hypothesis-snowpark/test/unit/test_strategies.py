from hypothesis import given, settings
from snowflake.snowpark import DataFrame
from snowflake.snowpark.types import StructField, BooleanType, StringType, IntegerType

from snowflake.hypothesis_snowpark import snowpark_dataframe


@given(
    df=snowpark_dataframe(
        columns=[
            StructField("A", IntegerType()),
            StructField("B", StringType()),
            StructField("C", BooleanType()),
        ],
        min_rows=1,
        max_rows=10,
    )
)
@settings(max_examples=5)
def test_snowpark_dataframe_strategy(df: DataFrame):
    assert isinstance(df, DataFrame)
    assert len(df.schema.fields) == 3
    assert 1 <= df.count() <= 10
    df.show()

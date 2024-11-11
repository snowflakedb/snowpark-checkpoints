from hypothesis import given
from snowflake.snowpark import DataFrame
from snowflake.hypothesis_snowpark import snowpark_dataframe


@given(df=snowpark_dataframe())
def test_with_snowpark_dataframe(df: DataFrame):
    assert df.count() == 5

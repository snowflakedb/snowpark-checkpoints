from hypothesis import given
from snowflake.snowpark import DataFrame
from snowflake.snowpark.types import StructField, BooleanType, StringType

from snowflake.hypothesis_snowpark import snowpark_dataframe


@given(df=snowpark_dataframe(
    columns=[StructField("id", BooleanType()),
             StructField("name", StringType())]))
def test_with_snowpark_dataframe(df: DataFrame):
    df.show()
    assert df.count() >= 5

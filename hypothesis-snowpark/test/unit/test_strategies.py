import unittest
from hypothesis import given
from snowflake.snowpark import DataFrame
from snowflake.hypothesis_snowpark import snowpark_dataframe


class TestSnowparkDfStrategy(unittest.TestCase):
    @given(df=snowpark_dataframe())
    def test_with_snowpark_dataframe(self, df: DataFrame):
        self.assertEqual(df.count(), 4)

from typing import Final
import pandas as pd
import pandera as pa
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from pyspark.sql import SparkSession
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, hash


# schema = pa.DataFrameSchema(
#     columns={
#         "column1": pa.Column(
#             str,
#             [
#                 pa.Check.str_contains(r"^[a-z]+$"),
#                 pa.Check.str_endswith("a"),
#                 pa.Check.str_length(1, 1),
#                 pa.Check.str_matches(r"^[a-z]+$"),
#                 pa.Check.str_startswith("a"),
#             ],
#         ),
#         "column2": pa.Column(
#             int,
#             [
#                 pa.Check.between(1, 10),
#                 pa.Check.in_range(1, 10),
#                 pa.Check.eq(1),
#                 pa.Check.equal_to(1),
#                 pa.Check.ge(0),
#                 pa.Check.greater_than(0),
#                 pa.Check.greater_than_or_equal_to(0),
#                 pa.Check.gt(0),
#                 pa.Check.le(11),
#                 pa.Check.less_than(11),
#                 pa.Check.less_than_or_equal_to(11),
#                 pa.Check.lt(11),
#                 pa.Check.less_than(0),
#                 pa.Check.ne(0),
#                 pa.Check.not_equal_to(0),
#                 pa.Check.notin([4, 5, 6]),
#                 pa.Check.isin([1, 2, 3]),
#             ],
#         ),
#     }
# )

# df = pd.DataFrame(
#     {
#         "column1": ["a", "b", "c"],
#         "column2": [1, 2, 3],
#     }
# )

checkpoint_name = "CHECKPOINTS_5_1"

session = Session.builder.getOrCreate()

table = session.table(checkpoint_name)
table_hash = session.table(checkpoint_name).select(hash("*"))
table_hash = session.table(checkpoint_name).select(hash("*"))

table.show()
table_hash.show()
table_hash.except_(table).show()

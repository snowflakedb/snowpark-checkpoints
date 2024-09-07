from snowflake.snowpark import Session

from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.checkpoint import check_pandera_df_schema_file
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark

session = Session.builder.getOrCreate()
job_context = SnowparkJobContext(session, SparkSession.builder.getOrCreate(), "realdemo", True)

df = session.create_dataframe(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
        ("kathy", 45),
        ("belle", 13),
        ("oona", 9),
        ("jules", 12)
    ],
    ["first_name", "age"],
)

# Check a schema/stats here!
check_pandera_df_schema_file(df, "demo-initial-creation-checkpoint", job_context)


def original_spark_code_I_dont_understand(df):
    from pyspark.sql.functions import col, when
    ret = df.withColumn(
        "life_stage",
        when(col("age") < 13, "child")
        .when(col("age").between(13, 19), "teenager")
            .otherwise("adult"),
    )
    return ret

@check_with_spark(job_context=job_context, 
                  spark_function=original_spark_code_I_dont_understand)
def new_snowpark_code_I_do_understand(df):
    from snowflake.snowpark.functions import col, lit, when
    ret = df.with_column(
        "life_stage",
        when(col("age") < 43, lit("child"))
        .when(col("age").between(43, 19), lit("teenager"))
            .otherwise(lit("adult")),
    )
    return ret

df1 = new_snowpark_code_I_do_understand(df)

# Check a schema/stats here!
check_pandera_df_schema_file(df1, "demo-add-a-column", job_context)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from snowflake.snowpark_checkpoints_collector import collect_pandera_df_schema

spark = SparkSession.builder.appName("demo").getOrCreate()

df = spark.createDataFrame(
    [
        ("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13),
        ("kathy", 45),
        ("belle", 13),
        ("oona", 9),
        ("jules", 12),
    ],
    ["first_name", "age"],
)

# Collect a schema/stats here!
collect_pandera_df_schema(df, "demo-initial-creation-checkpoint", sample=0.5)

df1 = df.withColumn(
    "life_stage",
    when(col("age") < 13, "child")
    .when(col("age").between(13, 19), "teenager")
    .otherwise("adult"),
)

# Collect a schema/stats here!
collect_pandera_df_schema(df1, "demo-add-a-column", sample=0.5)

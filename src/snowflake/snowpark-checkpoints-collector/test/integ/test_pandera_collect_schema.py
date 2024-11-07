
from pyspark.sql import SparkSession
import pytest
import pandas as pd
import numpy as np
import pandera as pa
from snowflake.snowpark_checkpoints_collector import collect_pandera_df_schema;

@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()

def test_collect_from_df(spark_session):
    data_df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    pyspark_df = spark_session.createDataFrame(data_df)
    collect_pandera_df_schema(pyspark_df, checkpoint_name="testdf", sample=0.1)
    output = open(f"snowpark-testdf-schema.json", "r")
    result = pa.DataFrameSchema.from_json(output)
    result.validate(pyspark_df.sample(0.1).toPandas())
    


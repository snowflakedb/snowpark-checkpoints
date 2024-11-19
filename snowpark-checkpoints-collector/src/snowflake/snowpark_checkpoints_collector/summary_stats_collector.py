import os.path

from numpy import dtype
import pandas
import numpy as np
from pyspark.sql import DataFrame as SparkDataFrame
import pandera as pa
from pandera import DataFrameSchema, Column, Check

def collect_pandera_input_schema(df:SparkDataFrame):
    pass

def collect_pandera_output_schema(df:SparkDataFrame):
    pass

def collect_pandera_df_schema(df:SparkDataFrame, 
                              checkpoint_name, 
                              sample=0.1,
                              file_dir = None):
    describe_df = df.describe().toPandas().set_index('summary')
    # use infer schema on a sample to set most values
    # this may be error prone

    sampled_df = df.sample(sample).toPandas()
    sampled_df.index = np.ones(sampled_df.count().iloc[0])

    schema = pa.infer_schema(sampled_df)
    # fix up the min/max constraints
    for col in schema.columns:
        col_dtype=schema.columns[col].dtype.type
        if col_dtype == 'empty':
            continue
        if (col == "index"):
            schema.columns[col].checks = []
        elif (pandas.api.types.is_numeric_dtype(col_dtype)):
            schema.columns[col].checks = [Check.less_than_or_equal_to(float(describe_df.loc['max'][col])),
                                        Check.greater_than_or_equal_to(float(describe_df.loc['min'][col]))]

    file_name = f"snowpark-{checkpoint_name}-schema.json"
    file_path = os.path.join(file_dir, file_name) if file_dir else file_name
    f = open(file_path, "w")
    f.write(schema.to_json())
    f.close()

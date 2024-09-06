
from numpy import dtype
import pandas
from pyspark.sql import DataFrame as SparkDataFrame
import pandera as pa
from pandera import DataFrameSchema, Column, Check

def collect_pandera_input_schema(df:SparkDataFrame):
    pass

def collect_pandera_output_schema(df:SparkDataFrame):
    pass

def collect_pandera_df_schema(df:SparkDataFrame, 
                              checkpoint_name, 
                              sample=0.1):
    describe_df = df.describe().toPandas().set_index('summary')
    # use infer schema on a sample to set most values
    # this may be error prone
    sampled_df = df.sample(sample).toPandas().reset_index()
    schema = pa.infer_schema(sampled_df)
    schema = schema.remove_columns(["index"])
    # fix up the min/max constraints
    for col in schema.columns:
        col_dtype=schema.columns[col].dtype.type
        if (col == "index"):
            schema.columns[col].checks = []
        elif (pandas.api.types.is_numeric_dtype(col_dtype)):
            schema.columns[col].checks = [Check.less_than_or_equal_to(float(describe_df.loc['max'][col])),
                                        Check.greater_than_or_equal_to(float(describe_df.loc['min'][col]))]
            
    f = open(f"snowpark-{checkpoint_name}-schema.json", "w")
    f.write(schema.to_json())
    f.close()

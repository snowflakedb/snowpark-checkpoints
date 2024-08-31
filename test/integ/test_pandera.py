from numpy import int8
import pandas as pd
from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from snowflake.snowpark_checkpoints.checkpoint import check_input_with_pandera, check_output_with_pandera
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit 

def test_pandera_input():
    df = pd.DataFrame({
        "COLUMN1": [1, 4, 0, 10, 9],
        "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
    })

    in_schema = DataFrameSchema({
        "COLUMN1": Column(int8,
                        Check(lambda x: 0 <= x <= 5, element_wise=True)),
        "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
    })

    @check_input_with_pandera(in_schema)
    def preprocessor(dataframe:SnowparkDataFrame):
        dataframe["column3"] = dataframe["column1"] + dataframe["column2"]
        return dataframe
    
    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)
    try:
        preprocessed_df = preprocessor(sp_df)
        assert(False)
    except:
        pass

    
    
def test_pandera_output():
    df = pd.DataFrame({
        "COLUMN1": [1, 4, 0, 10, 9],
        "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
    })

    out_schema = DataFrameSchema({
        "COLUMN1": Column(int8,
                        Check(lambda x: 0 <= x <= 10, element_wise=True)),
        "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
    })

    @check_output_with_pandera(out_schema)
    def preprocessor(dataframe:SnowparkDataFrame):
        return dataframe.with_column("COLUMN1", lit('Some bad data yo'))
    
    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    try:
        preprocessed_df = preprocessor(sp_df)
        assert(False)
    except:
        pass
    
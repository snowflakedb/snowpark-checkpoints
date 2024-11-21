from numpy import int8
import pandas as pd
from pandera import DataFrameSchema, Column, Check
import snowflake
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from snowflake.snowpark_checkpoints.checkpoint import (
    check_df_schema_file,
    check_df_schema,
    check_output_schema,
    check_input_schema,
)
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit


def test_input():
    df = pd.DataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    in_schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 5, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
        }
    )

    @check_input_schema(in_schema)
    def preprocessor(dataframe: SnowparkDataFrame):
        dataframe["column3"] = dataframe["column1"] + dataframe["column2"]
        return dataframe

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)
    try:
        preprocessed_df = preprocessor(sp_df)
        assert False
    except:
        pass


def test_output():
    df = pd.DataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    out_schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
        }
    )

    @check_output_schema(out_schema)
    def preprocessor(dataframe: SnowparkDataFrame):
        return dataframe.with_column("COLUMN1", lit("Some bad data yo"))

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    try:
        preprocessed_df = preprocessor(sp_df)
        assert False
    except:
        pass


def test_df_check():
    df = pd.DataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
        }
    )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)
    check_df_schema(sp_df, schema)


def test_df_check_from_file():
    df = pd.DataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
        }
    )
    checkpoint_name = "testdf"
    output_file = open(f"snowpark-{checkpoint_name}-schema.json", "w")
    output_file.write(schema.to_json())
    output_file.close()

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)
    check_df_schema_file(sp_df, checkpoint_name)

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
import os
from numpy import int8
from pandas import DataFrame as PandasDataFrame
from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from snowflake.snowpark_checkpoints.checkpoint import (
    check_dataframe_schema_file,
    check_dataframe_schema,
    check_output_schema,
    check_input_schema,
)
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit

from snowflake.snowpark_checkpoints.utils.constant import (
    CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME,
    SKIP_ALL,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_FORMAT_NAME,
)


def test_input():
    df = PandasDataFrame(
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
    df = PandasDataFrame(
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
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
        }
    )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    check_dataframe_schema(sp_df, schema)


def test_df_check_from_file():
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
            "COLUMN3": [True, False, True, False, True],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check.between(0, 10)),
            "COLUMN2": Column(float, Check.between(-20.5, -1.0)),
        }
    )

    schema_data = {
        "pandera_schema": json.loads(schema.to_json()),
        "custom_data": {
            "columns": [
                {
                    "name": "COLUMN1",
                    "type": "integer",
                    "rows_count": 5,
                    "rows_not_null_count": 5,
                    "rows_null_count": 0,
                    "min": 0,
                    "max": 10,
                    "mean": 4.8,
                    "decimal_precision": 0,
                    "margin_error": 4.0693979898752,
                },
                {
                    "name": "COLUMN2",
                    "type": "float",
                    "rows_count": 5,
                    "rows_not_null_count": 5,
                    "rows_null_count": 0,
                    "min": -20.4,
                    "max": -1.3,
                    "mean": -7.22,
                    "decimal_precision": 1,
                    "margin_error": 7.3428604780426,
                },
            ],
        },
    }

    checkpoint_name = "test_checkpoint"

    current_directory_path = os.getcwd()

    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_FORMAT_NAME
    )

    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path)

    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME.format(
        checkpoint_name
    )

    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)

    with open(checkpoint_file_path, "w") as output_file:
        output_file.write(json.dumps(schema_data))

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    check_dataframe_schema_file(sp_df, checkpoint_name)


def test_df_check_custom_check():
    df = PandasDataFrame(
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
    check_dataframe_schema(
        sp_df,
        schema,
        custom_checks={
            "COLUMN1": [
                Check(lambda x: x.shape[0] == 5),
                Check(lambda x: x.shape[1] == 2),
            ],
            "COLUMN2": [Check(lambda x: x.shape[0] == 5)],
        },
    )

    assert len(schema.columns["COLUMN1"].checks) == 3
    assert len(schema.columns["COLUMN2"].checks) == 2


def test_df_check_skip_check():
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check.between(0, 10, element_wise=True)),
            "COLUMN2": Column(
                float,
                [
                    Check.greater_than(-20.5),
                    Check.less_than(-1.0),
                    Check(lambda x: x < -1.2),
                ],
            ),
        }
    )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)
    check_dataframe_schema(
        sp_df,
        schema,
        skip_checks={"COLUMN1": [SKIP_ALL], "COLUMN2": ["greater_than", "less_than"]},
    )

    assert len(schema.columns["COLUMN1"].checks) == 0
    assert len(schema.columns["COLUMN2"].checks) == 1

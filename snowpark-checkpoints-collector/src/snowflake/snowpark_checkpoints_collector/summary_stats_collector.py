#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json
import os

import pandera as pa

from pyspark.sql import DataFrame as SparkDataFrame

from snowflake.snowpark_checkpoints_collector.collection_common import (
    CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT,
    COLUMNS_KEY,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    EMPTY_DATAFRAME_WITHOUT_SCHEMA_ERROR_MESSAGE,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    STRING_COLUMN_TYPE,
)
from snowflake.snowpark_checkpoints_collector.column_collection import (
    ColumnCollectorManager,
)
from snowflake.snowpark_checkpoints_collector.column_pandera_checks import (
    PanderaColumnChecksManager,
)


def collect_input_schema(df: SparkDataFrame) -> None:
    """Collect and return the input schema of a Spark DataFrame.

    Args:
        df (SparkDataFrame): The input Spark DataFrame.

    Returns:
        Optional[StructType]: The schema of the input DataFrame.

    """
    pass


def collect_output_schema(df: SparkDataFrame) -> None:
    """Collect and return the output schema of a Spark DataFrame.

    Args:
        df (SparkDataFrame): The output Spark DataFrame.

    Returns:
        Optional[StructType]: The schema of the output DataFrame.

    """
    pass


def collect_dataframe_checkpoint(
    df: SparkDataFrame, checkpoint_name, sample=1.0
) -> None:
    """Collect a DataFrame checkpoint.

    Args:
        df (SparkDataFrame): The input Spark DataFrame to collect.
        checkpoint_name (str): The name of the checkpoint.
        sample (float, optional): Fraction of DataFrame to sample for schema inference.
            Defaults to 1.0.

    Raises:
        Exception: It is not possible to collect a empty DataFrame without schema.

    """
    try:
        if _is_empty_dataframe_without_schema(df):
            raise Exception(EMPTY_DATAFRAME_WITHOUT_SCHEMA_ERROR_MESSAGE)

        is_empty_df_with_string_column = _is_empty_dataframe_with_string_column(df)

        column_type_dict = _get_spark_column_types(df)
        column_name_collection = df.schema.names
        source_df = df.sample(sample)

        if source_df.isEmpty():
            source_df = df

        pandas_df = source_df.toPandas()
        pandera_infer_schema = (
            pa.infer_schema(pandas_df) if not is_empty_df_with_string_column else {}
        )
        column_custom_data_collection = []
        column_collector_manager = ColumnCollectorManager()
        column_pandera_checks_manager = PanderaColumnChecksManager()

        for column in column_name_collection:
            column_type = column_type_dict[column]
            column_has_data = len(pandas_df[column]) > 0

            if not column_has_data:
                custom_data = column_collector_manager.collect_empty_custom_data(
                    column, column_type, pandas_df[column]
                )
                column_custom_data_collection.append(custom_data)
                continue

            pandera_column = pandera_infer_schema.columns[column]
            column_pandera_checks_manager.add_checks_column(
                column, column_type, pandas_df, pandera_column
            )

            custom_data = column_collector_manager.collect_column(
                column, column_type, pandas_df[column]
            )
            column_custom_data_collection.append(custom_data)

        pandera_infer_schema_dict = (
            json.loads(pandera_infer_schema.to_json())
            if not is_empty_df_with_string_column
            else {}
        )
        dataframe_custom_column_data = {COLUMNS_KEY: column_custom_data_collection}

        dataframe_schema_contract = {
            DATAFRAME_PANDERA_SCHEMA_KEY: pandera_infer_schema_dict,
            DATAFRAME_CUSTOM_DATA_KEY: dataframe_custom_column_data,
        }

        dataframe_schema_contract_json = json.dumps(dataframe_schema_contract)

        _generate_json_checkpoint_file(checkpoint_name, dataframe_schema_contract_json)

    except Exception as err:
        error_message = str(err)
        raise Exception(error_message) from None


def _get_spark_column_types(df: SparkDataFrame) -> dict[str, any]:
    schema = df.schema
    column_type_collection = {}
    for field in schema.fields:
        column_name = field.name
        type_name = field.dataType.typeName()
        column_type_collection[column_name] = type_name
    return column_type_collection


def _is_empty_dataframe_without_schema(df: SparkDataFrame) -> bool:
    is_empty = df.isEmpty()
    has_schema = len(df.schema.fields) > 0
    return is_empty and not has_schema


def _is_empty_dataframe_with_string_column(df: SparkDataFrame):
    is_empty = df.isEmpty()
    if not is_empty:
        return False

    for field in df.schema.fields:
        if field.dataType.typeName() == STRING_COLUMN_TYPE:
            return True

    return False


def _generate_json_checkpoint_file(checkpoint_name, dataframe_schema_contract) -> None:
    current_directory_path = os.getcwd()
    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path)

    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )
    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)
    f = open(checkpoint_file_path, "w")
    f.write(dataframe_schema_contract)

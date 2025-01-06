#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json
import os

from typing import Optional

import pandera as pa

from pyspark.sql import DataFrame as SparkDataFrame

from snowflake.snowpark_checkpoints_collector.collection_common import (
    CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT,
    CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT,
    COLUMNS_KEY,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    DECIMAL_COLUMN_TYPE,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    STRING_COLUMN_TYPE,
    CheckpointMode,
)
from snowflake.snowpark_checkpoints_collector.column_collection import (
    ColumnCollectorManager,
)
from snowflake.snowpark_checkpoints_collector.column_pandera_checks import (
    PanderaColumnChecksManager,
)
from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)
from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
    get_checkpoint_mode,
    get_checkpoint_sample,
    is_checkpoint_enabled,
)
from snowflake.snowpark_checkpoints_collector.utils.telemetry import report_telemetry


def collect_dataframe_checkpoint(
    df: SparkDataFrame,
    checkpoint_name,
    sample: Optional[float] = None,
    mode: Optional[CheckpointMode] = None,
) -> None:
    """Collect a DataFrame checkpoint.

    Args:
        df (SparkDataFrame): The input Spark DataFrame to collect.
        checkpoint_name (str): The name of the checkpoint.
        sample (float, optional): Fraction of DataFrame to sample for schema inference.
            Defaults to 1.0.
        mode (CheckpointMode): The mode to execution the collection.
            Defaults to CheckpointMode.Schema

    Raises:
        Exception: It is not possible to collect an empty DataFrame without schema.
        Exception: Invalid mode value.

    """
    try:
        if is_checkpoint_enabled(checkpoint_name):

            _sample = get_checkpoint_sample(checkpoint_name, sample)

            if _is_empty_dataframe_without_schema(df):
                raise Exception(
                    "It is not possible to collect an empty DataFrame without schema"
                )

            _mode = get_checkpoint_mode(checkpoint_name, mode)

            if _mode == CheckpointMode.SCHEMA:
                column_type_dict = _get_spark_column_types(df)
                _collect_dataframe_checkpoint_mode_schema(
                    checkpoint_name, df, _sample, column_type_dict
                )

            elif _mode == CheckpointMode.DATAFRAME:
                snow_connection = SnowConnection()
                _collect_dataframe_checkpoint_mode_dataframe(
                    checkpoint_name, df, snow_connection
                )

            else:
                raise Exception("Invalid mode value.")

    except Exception as err:
        error_message = str(err)
        raise Exception(error_message) from err


@report_telemetry(params_list=["column_type_dict"])
def _collect_dataframe_checkpoint_mode_schema(
    checkpoint_name: str,
    df: SparkDataFrame,
    sample: float,
    column_type_dict: dict[str, any],
) -> None:
    source_df = df.sample(sample)
    if source_df.isEmpty():
        source_df = df
    pandas_df = source_df.toPandas()
    is_empty_df_with_string_column = _is_empty_dataframe_with_string_column(df)
    pandera_infer_schema = (
        pa.infer_schema(pandas_df) if not is_empty_df_with_string_column else {}
    )

    column_name_collection = df.schema.names
    columns_to_remove_from_pandera_schema_collection = []
    column_custom_data_collection = []
    column_collector_manager = ColumnCollectorManager()
    column_pandera_checks_manager = PanderaColumnChecksManager()

    for column in column_name_collection:
        column_type = column_type_dict[column]
        is_empty_column = len(pandas_df[column]) == 0
        is_column_to_remove_from_pandera_schema = (
            _is_column_to_remove_from_pandera_schema(column_type)
        )

        if is_column_to_remove_from_pandera_schema:
            columns_to_remove_from_pandera_schema_collection.append(column)

        if is_empty_column:
            custom_data = column_collector_manager.collect_empty_custom_data(
                column, column_type, pandas_df[column]
            )
            column_custom_data_collection.append(custom_data)
            continue

        pandera_column = pandera_infer_schema.columns[column]
        pandera_column.checks = []
        column_pandera_checks_manager.add_checks_column(
            column, column_type, pandas_df, pandera_column
        )

        custom_data = column_collector_manager.collect_column(
            column, column_type, pandas_df[column]
        )
        column_custom_data_collection.append(custom_data)

    pandera_infer_schema_dict = _get_pandera_infer_schema_as_dict(
        pandera_infer_schema,
        is_empty_df_with_string_column,
        columns_to_remove_from_pandera_schema_collection,
    )

    dataframe_custom_column_data = {COLUMNS_KEY: column_custom_data_collection}
    dataframe_schema_contract = {
        DATAFRAME_PANDERA_SCHEMA_KEY: pandera_infer_schema_dict,
        DATAFRAME_CUSTOM_DATA_KEY: dataframe_custom_column_data,
    }

    dataframe_schema_contract_json = json.dumps(dataframe_schema_contract)
    _generate_json_checkpoint_file(checkpoint_name, dataframe_schema_contract_json)


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


def _is_column_to_remove_from_pandera_schema(column_type) -> bool:
    is_decimal_type = column_type == DECIMAL_COLUMN_TYPE
    return is_decimal_type


def _get_pandera_infer_schema_as_dict(
    pandera_infer_schema, is_empty_df_with_string_column, columns_to_remove_collection
) -> dict[str, any]:
    if is_empty_df_with_string_column:
        return {}

    pandera_infer_schema_dict = json.loads(pandera_infer_schema.to_json())
    for column in columns_to_remove_collection:
        del pandera_infer_schema_dict[COLUMNS_KEY][column]

    return pandera_infer_schema_dict


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
    with open(checkpoint_file_path, "w") as f:
        f.write(dataframe_schema_contract)


def _collect_dataframe_checkpoint_mode_dataframe(
    checkpoint_name, df: SparkDataFrame, snow_connection
) -> None:
    _generate_parquet_checkpoint_file(checkpoint_name, df)
    _upload_to_snowflake(checkpoint_name, snow_connection)


def _generate_parquet_checkpoint_file(checkpoint_name, df: SparkDataFrame) -> None:
    output_directory_path = _get_output_directory_path()

    checkpoint_file_name = CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )
    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)
    df.write.parquet(checkpoint_file_path, mode="overwrite")


def _get_output_directory_path() -> str:
    current_directory_path = os.getcwd()
    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path)

    return output_directory_path


def _upload_to_snowflake(checkpoint_name, snow_connection) -> None:
    try:
        output_directory_path = _get_output_directory_path()
        checkpoint_file_name = CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT.format(
            checkpoint_name
        )
        output_path = os.path.join(output_directory_path, checkpoint_file_name)
        snow_connection.upload_to_snowflake(
            checkpoint_name, checkpoint_file_name, output_path
        )

    except Exception as err:
        error_message = str(err)
        raise Exception(error_message) from err

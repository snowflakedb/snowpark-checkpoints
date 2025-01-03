#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json
import os

from typing import Optional

import pandera as pa

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType as SparkDoubleType
from pyspark.sql.types import StringType as SparkStringType
from pyspark.sql.types import StructField
from snowflake.snowpark_checkpoints_collector.collection_common import (
    CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT,
    CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT,
    COLUMNS_KEY,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    DECIMAL_COLUMN_TYPE,
    PANDAS_OBJECT_TYPE_COLLECTION,
    CheckpointMode,
)
from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
    CollectionPointResultManager,
    CollectionResult,
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
from snowflake.snowpark_checkpoints_collector.utils import file_utils
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
    output_path: Optional[str] = None,
) -> None:
    """Collect a DataFrame checkpoint.

    Args:
        df (SparkDataFrame): The input Spark DataFrame to collect.
        checkpoint_name (str): The name of the checkpoint.
        sample (float, optional): Fraction of DataFrame to sample for schema inference.
            Defaults to 1.0.
        mode (CheckpointMode): The mode to execution the collection.
            Defaults to CheckpointMode.Schema
        output_path (str, optional): The output path to save the checkpoint.
            Defaults to Current working Directory.

    Raises:
        Exception: It is not possible to collect an empty DataFrame without schema.
        Exception: Invalid mode value.

    """
    collection_point_file_path = file_utils.get_collection_point_source_file_path()
    collection_point_line_of_code = file_utils.get_collection_point_line_of_code()
    collection_point_result = CollectionPointResult(
        collection_point_file_path, collection_point_line_of_code, checkpoint_name
    )
    file_utils.create_output_directory()

    try:
        if is_checkpoint_enabled(checkpoint_name):

            _sample = get_checkpoint_sample(checkpoint_name, sample)

            _mode = get_checkpoint_mode(checkpoint_name, mode)

            if _mode == CheckpointMode.SCHEMA:
                column_type_dict = _get_spark_column_types(df)
                _collect_dataframe_checkpoint_mode_schema(
                    checkpoint_name, df, _sample, column_type_dict, output_path
                )

            elif _mode == CheckpointMode.DATAFRAME:
                snow_connection = SnowConnection()
                _collect_dataframe_checkpoint_mode_dataframe(
                    checkpoint_name, df, snow_connection, output_path
                )

            else:
                raise Exception("Invalid mode value.")

            collection_point_result.result = CollectionResult.PASS

    except Exception as err:
        collection_point_result.result = CollectionResult.FAIL
        error_message = str(err)
        raise Exception(error_message) from err

    finally:
        collection_point_result_manager = CollectionPointResultManager()
        collection_point_result_manager.add_result(collection_point_result)


@report_telemetry(params_list=["column_type_dict"])
def _collect_dataframe_checkpoint_mode_schema(
    checkpoint_name: str,
    df: SparkDataFrame,
    sample: float,
    column_type_dict: dict[str, any],
    output_path: Optional[str] = None,
) -> None:
    source_df = df.sample(sample)
    if source_df.isEmpty():
        source_df = df
    pandas_df = source_df.toPandas()
    is_empty_df_with_object_column = _is_empty_dataframe_with_object_column(df)
    pandera_infer_schema = (
        pa.infer_schema(pandas_df) if not is_empty_df_with_object_column else {}
    )

    column_name_collection = df.schema.names
    columns_to_remove_from_pandera_schema_collection = []
    column_custom_data_collection = []
    column_collector_manager = ColumnCollectorManager()
    column_pandera_checks_manager = PanderaColumnChecksManager()

    for column in column_name_collection:
        struct_field_column = column_type_dict[column]
        column_type = struct_field_column.dataType.typeName()
        is_empty_column = len(pandas_df[column].dropna()) == 0
        is_column_to_remove_from_pandera_schema = (
            _is_column_to_remove_from_pandera_schema(column_type)
        )

        if is_column_to_remove_from_pandera_schema:
            columns_to_remove_from_pandera_schema_collection.append(column)

        if is_empty_column:
            custom_data = column_collector_manager.collect_empty_custom_data(
                column, struct_field_column, pandas_df[column]
            )
            column_custom_data_collection.append(custom_data)
            continue

        pandera_column = pandera_infer_schema.columns[column]
        pandera_column.checks = []
        column_pandera_checks_manager.add_checks_column(
            column, column_type, pandas_df, pandera_column
        )

        custom_data = column_collector_manager.collect_column(
            column, struct_field_column, pandas_df[column]
        )
        column_custom_data_collection.append(custom_data)

    pandera_infer_schema_dict = _get_pandera_infer_schema_as_dict(
        pandera_infer_schema,
        is_empty_df_with_object_column,
        columns_to_remove_from_pandera_schema_collection,
    )

    dataframe_custom_column_data = {COLUMNS_KEY: column_custom_data_collection}
    dataframe_schema_contract = {
        DATAFRAME_PANDERA_SCHEMA_KEY: pandera_infer_schema_dict,
        DATAFRAME_CUSTOM_DATA_KEY: dataframe_custom_column_data,
    }

    dataframe_schema_contract_json = json.dumps(dataframe_schema_contract)
    _generate_json_checkpoint_file(
        checkpoint_name, dataframe_schema_contract_json, output_path
    )


def _get_spark_column_types(df: SparkDataFrame) -> dict[str, StructField]:
    schema = df.schema
    column_type_collection = {}
    for field in schema.fields:
        column_name = field.name
        column_type_collection[column_name] = field
    return column_type_collection


def _is_empty_dataframe_without_schema(df: SparkDataFrame) -> bool:
    is_empty = df.isEmpty()
    has_schema = len(df.schema.fields) > 0
    return is_empty and not has_schema


def _is_empty_dataframe_with_object_column(df: SparkDataFrame):
    is_empty = df.isEmpty()
    if not is_empty:
        return False

    for field in df.schema.fields:
        if field.dataType.typeName() in PANDAS_OBJECT_TYPE_COLLECTION:
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
    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )
    output_directory_path = file_utils.get_output_directory_path()
    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)
    with open(checkpoint_file_path, "w") as f:
        f.write(dataframe_schema_contract)


def _collect_dataframe_checkpoint_mode_dataframe(
    checkpoint_name: str,
    df: SparkDataFrame,
    snow_connection: SnowConnection,
    output_path: Optional[str] = None,
) -> None:
    current_directory_path = output_path if output_path else os.getcwd()
    output_path = os.path.join(
        current_directory_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        checkpoint_name,
    )

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    _generate_parquet_checkpoint_file(df, output_path)
    _create_snowflake_table_from_parquet(checkpoint_name, output_path, snow_connection)


def _generate_parquet_checkpoint_file(
    spark_df: SparkDataFrame, output_path: str
) -> None:
    _generate_parquet_checkpoint_file(checkpoint_name, df)
    _upload_to_snowflake(checkpoint_name, snow_connection)


def _generate_parquet_checkpoint_file(checkpoint_name, df: SparkDataFrame) -> None:
    output_directory_path = file_utils.get_output_directory_path()

    checkpoint_file_name = CHECKPOINT_PARQUET_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )
    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)
    df.write.parquet(checkpoint_file_path, mode="overwrite")
    # Convert Float to Double to avoid precision problems. Spark parquet use IEEE 32-bit floating point values,
    # while Snowflake uses IEEE 64-bit floating point values.
    new_cols = [
        (
            col(c).cast(SparkStringType()).cast(SparkDoubleType()).alias(c)
            if t == "float"
            else col(c)
        )
        for (c, t) in spark_df.dtypes
    ]
    converted_df = spark_df.select(new_cols)
    converted_df.write.parquet(output_path, mode="overwrite")


def _create_snowflake_table_from_parquet(
    table_name: str, input_path: str, snow_connection: SnowConnection
) -> None:
    try:
        snow_connection.create_snowflake_table_from_local_parquet(
            table_name, input_path
        )

    except Exception as err:
        error_message = str(err)
        raise Exception(error_message) from err


def _get_output_directory_path() -> str:
    current_directory_path = os.getcwd()
    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path)

    return output_directory_path
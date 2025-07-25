# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import logging
import os

from typing import Optional

import pandas
import pandera as pa

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import BinaryType as SparkBinaryType
from pyspark.sql.types import BooleanType as SparkBooleanType
from pyspark.sql.types import DateType as SparkDateType
from pyspark.sql.types import DoubleType as SparkDoubleType
from pyspark.sql.types import FloatType as SparkFloatType
from pyspark.sql.types import IntegerType as SparkIntegerType
from pyspark.sql.types import StringType as SparkStringType
from pyspark.sql.types import StructField as SparkStructField
from pyspark.sql.types import TimestampType as SparkTimestampType

from snowflake.snowpark_checkpoints_collector.collection_common import (
    CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT,
    COLUMNS_KEY,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    DECIMAL_COLUMN_TYPE,
    DOT_PARQUET_EXTENSION,
    INTEGER_TYPE_COLLECTION,
    NULL_COLUMN_TYPE,
    PANDAS_FLOAT_TYPE,
    PANDAS_LONG_TYPE,
    PANDAS_OBJECT_TYPE_COLLECTION,
    PANDAS_STRING_TYPE,
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
from snowflake.snowpark_checkpoints_collector.io_utils.io_file_manager import (
    get_io_file_manager,
)
from snowflake.snowpark_checkpoints_collector.snow_connection_model import (
    SnowConnection,
)
from snowflake.snowpark_checkpoints_collector.utils import (
    checkpoint_name_utils,
    file_utils,
)
from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
    get_checkpoint_mode,
    get_checkpoint_sample,
    is_checkpoint_enabled,
)
from snowflake.snowpark_checkpoints_collector.utils.logging_utils import log
from snowflake.snowpark_checkpoints_collector.utils.telemetry import report_telemetry


LOGGER = logging.getLogger(__name__)

default_null_types = {
    SparkIntegerType(): 0,
    SparkFloatType(): 0.0,
    SparkDoubleType(): 0.0,
    SparkStringType(): "",
    SparkBooleanType(): False,
    SparkTimestampType(): None,
    SparkDateType(): None,
}


@log
def collect_dataframe_checkpoint(
    df: SparkDataFrame,
    checkpoint_name: str,
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
        Exception: Invalid mode value.
        Exception: Invalid checkpoint name. Checkpoint names must only contain alphanumeric characters
                   , underscores and dollar signs.

    """
    normalized_checkpoint_name = checkpoint_name_utils.normalize_checkpoint_name(
        checkpoint_name
    )
    if normalized_checkpoint_name != checkpoint_name:
        LOGGER.info(
            "Checkpoint name '%s' was normalized to '%s'",
            checkpoint_name,
            normalized_checkpoint_name,
        )
    is_valid_checkpoint_name = checkpoint_name_utils.is_valid_checkpoint_name(
        normalized_checkpoint_name
    )
    if not is_valid_checkpoint_name:
        raise Exception(
            f"Invalid checkpoint name: {normalized_checkpoint_name}. "
            "Checkpoint names must only contain alphanumeric characters, underscores and dollar signs."
        )
    if not is_checkpoint_enabled(normalized_checkpoint_name):
        raise Exception(
            f"Checkpoint '{normalized_checkpoint_name}' is disabled. Please enable it in the checkpoints.json file.",
            "In case you want to skip it, use the xcollect_dataframe_checkpoint method instead.",
        )

    LOGGER.info("Starting to collect checkpoint '%s'", normalized_checkpoint_name)
    LOGGER.debug("DataFrame size: %s rows", df.count())
    LOGGER.debug("DataFrame schema: %s", df.schema)

    collection_point_file_path = file_utils.get_collection_point_source_file_path()
    collection_point_line_of_code = file_utils.get_collection_point_line_of_code()
    collection_point_result = CollectionPointResult(
        collection_point_file_path,
        collection_point_line_of_code,
        normalized_checkpoint_name,
    )

    try:
        if _is_empty_dataframe_without_schema(df):
            raise Exception(
                "It is not possible to collect an empty DataFrame without schema"
            )

        _mode = get_checkpoint_mode(normalized_checkpoint_name, mode)

        if _mode == CheckpointMode.SCHEMA:
            column_type_dict = _get_spark_column_types(df)
            _sample = get_checkpoint_sample(normalized_checkpoint_name, sample)
            LOGGER.info(
                "Collecting checkpoint in %s mode using sample value %s",
                CheckpointMode.SCHEMA.name,
                _sample,
            )
            _collect_dataframe_checkpoint_mode_schema(
                normalized_checkpoint_name,
                df,
                _sample,
                column_type_dict,
                output_path,
            )
        elif _mode == CheckpointMode.DATAFRAME:
            LOGGER.info(
                "Collecting checkpoint in %s mode", CheckpointMode.DATAFRAME.name
            )
            snow_connection = SnowConnection()
            _collect_dataframe_checkpoint_mode_dataframe(
                normalized_checkpoint_name, df, snow_connection, output_path
            )
        else:
            raise Exception(f"Invalid mode value: {_mode}")

        collection_point_result.result = CollectionResult.PASS
        LOGGER.info(
            "Checkpoint '%s' collected successfully", normalized_checkpoint_name
        )

    except Exception as err:
        collection_point_result.result = CollectionResult.FAIL
        error_message = str(err)
        raise Exception(error_message) from err

    finally:
        collection_point_result_manager = CollectionPointResultManager(output_path)
        collection_point_result_manager.add_result(collection_point_result)


@log
def xcollect_dataframe_checkpoint(
    df: SparkDataFrame,
    checkpoint_name: str,
    sample: Optional[float] = None,
    mode: Optional[CheckpointMode] = None,
    output_path: Optional[str] = None,
) -> None:
    """Skips the collection of metadata from a Dataframe checkpoint.

    Args:
        df (SparkDataFrame): The input Spark DataFrame to skip.
        checkpoint_name (str): The name of the checkpoint.
        sample (float, optional): Fraction of DataFrame to sample for schema inference.
            Defaults to 1.0.
        mode (CheckpointMode): The mode to execution the collection.
            Defaults to CheckpointMode.Schema
        output_path (str, optional): The output path to save the checkpoint.
            Defaults to Current working Directory.

    Raises:
        Exception: Invalid mode value.
        Exception: Invalid checkpoint name. Checkpoint names must only contain alphanumeric characters,
                     underscores and dollar signs.

    """
    normalized_checkpoint_name = checkpoint_name_utils.normalize_checkpoint_name(
        checkpoint_name
    )
    if normalized_checkpoint_name != checkpoint_name:
        LOGGER.warning(
            "Checkpoint name '%s' was normalized to '%s'",
            checkpoint_name,
            normalized_checkpoint_name,
        )
    is_valid_checkpoint_name = checkpoint_name_utils.is_valid_checkpoint_name(
        normalized_checkpoint_name
    )
    if not is_valid_checkpoint_name:
        raise Exception(
            f"Invalid checkpoint name: {normalized_checkpoint_name}. "
            "Checkpoint names must only contain alphanumeric characters, underscores and dollar signs."
        )

    LOGGER.warning(
        "Checkpoint '%s' is disabled. Skipping collection.",
        normalized_checkpoint_name,
    )

    collection_point_file_path = file_utils.get_collection_point_source_file_path()
    collection_point_line_of_code = file_utils.get_collection_point_line_of_code()
    collection_point_result = CollectionPointResult(
        collection_point_file_path,
        collection_point_line_of_code,
        normalized_checkpoint_name,
    )

    collection_point_result.result = CollectionResult.SKIP
    collection_point_result_manager = CollectionPointResultManager(output_path)
    collection_point_result_manager.add_result(collection_point_result)


@report_telemetry(params_list=["column_type_dict"])
def _collect_dataframe_checkpoint_mode_schema(
    checkpoint_name: str,
    df: SparkDataFrame,
    sample: float,
    column_type_dict: dict[str, any],
    output_path: Optional[str] = None,
) -> None:
    df = normalize_missing_values(df)
    sampled_df = df.sample(sample)
    if sampled_df.isEmpty():
        LOGGER.warning("Sampled DataFrame is empty. Collecting full DataFrame.")
        sampled_df = df

    pandas_df = _to_pandas(sampled_df)
    is_empty_df_with_object_column = _is_empty_dataframe_with_object_column(df)
    if is_empty_df_with_object_column:
        LOGGER.debug(
            "DataFrame is empty with object column. Skipping Pandera schema inference."
        )
        pandera_infer_schema = {}
    else:
        LOGGER.debug("Inferring Pandera schema from DataFrame")
        pandera_infer_schema = pa.infer_schema(pandas_df)

    column_name_collection = df.schema.names
    columns_to_remove_from_pandera_schema_collection = []
    column_custom_data_collection = []
    column_collector_manager = ColumnCollectorManager()
    column_pandera_checks_manager = PanderaColumnChecksManager()

    for column_name in column_name_collection:
        struct_field_column = column_type_dict[column_name]
        column_type = struct_field_column.dataType.typeName()
        LOGGER.info("Collecting column '%s' of type '%s'", column_name, column_type)
        pyspark_column = df.select(col(column_name))

        is_column_to_remove_from_pandera_schema = (
            _is_column_to_remove_from_pandera_schema(column_type)
        )
        if is_column_to_remove_from_pandera_schema:
            columns_to_remove_from_pandera_schema_collection.append(column_name)

        is_empty_column = (
            pyspark_column.dropna().isEmpty() and column_type is not NULL_COLUMN_TYPE
        )
        if is_empty_column:
            LOGGER.debug("Column '%s' is empty.", column_name)
            custom_data = column_collector_manager.collect_empty_custom_data(
                column_name, struct_field_column, pyspark_column
            )
            column_custom_data_collection.append(custom_data)
            continue

        pandera_column = pandera_infer_schema.columns[column_name]
        pandera_column.checks = []
        column_pandera_checks_manager.add_checks_column(
            column_name, column_type, df, pandera_column
        )

        custom_data = column_collector_manager.collect_column(
            column_name, struct_field_column, pyspark_column
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


def normalize_missing_values(df: SparkDataFrame) -> SparkDataFrame:
    """Normalize missing values in a PySpark DataFrame to ensure consistent handling of NA values."""
    for field in df.schema.fields:
        default_value = default_null_types.get(field.dataType, None)
        if default_value is not None:
            df = df.fillna({field.name: default_value})
    return df


def _get_spark_column_types(df: SparkDataFrame) -> dict[str, SparkStructField]:
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
        LOGGER.debug("Removing column '%s' from Pandera schema", column)
        del pandera_infer_schema_dict[COLUMNS_KEY][column]

    return pandera_infer_schema_dict


def _generate_json_checkpoint_file(
    checkpoint_name, dataframe_schema_contract, output_path: Optional[str] = None
) -> None:
    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(
        checkpoint_name
    )
    output_directory_path = file_utils.get_output_directory_path(output_path)
    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)
    LOGGER.info("Writing DataFrame JSON schema file to '%s'", checkpoint_file_path)
    get_io_file_manager().write(checkpoint_file_path, dataframe_schema_contract)


@report_telemetry(params_list=["df"])
def _collect_dataframe_checkpoint_mode_dataframe(
    checkpoint_name: str,
    df: SparkDataFrame,
    snow_connection: SnowConnection,
    output_path: Optional[str] = None,
) -> None:
    output_path = file_utils.get_output_directory_path(output_path)
    parquet_directory = os.path.join(output_path, checkpoint_name)
    generate_parquet_for_spark_df(df, parquet_directory)
    _create_snowflake_table_from_parquet(
        checkpoint_name, parquet_directory, snow_connection
    )


def generate_parquet_for_spark_df(spark_df: SparkDataFrame, output_path: str) -> None:
    """Generate a parquet file from a Spark DataFrame.

    This function will  convert Float to Double to avoid precision problems.
    Spark parquet use IEEE 32-bit floating point values,
    while Snowflake uses IEEE 64-bit floating point values.

    Args:
        spark_df: dataframe to be saved as parquet
        output_path: path to save the parquet files.
    returns: None

    Raises:
        Exception: No parquet files were generated.

    """
    new_cols = [
        (
            col(c).cast(SparkStringType()).cast(SparkDoubleType()).alias(c)
            if t == "float"
            else col(c)
        )
        for (c, t) in spark_df.dtypes
    ]
    converted_df = spark_df.select(new_cols)

    if get_io_file_manager().folder_exists(output_path):
        LOGGER.warning(
            "Output directory '%s' already exists. Deleting it...", output_path
        )
        get_io_file_manager().remove_dir(output_path)

    LOGGER.info("Writing DataFrame to parquet files at '%s'", output_path)
    converted_df.write.parquet(output_path, mode="overwrite")

    target_dir = os.path.join(output_path, "**", f"*{DOT_PARQUET_EXTENSION}")
    parquet_files = get_io_file_manager().ls(target_dir, recursive=True)
    parquet_files_count = len(parquet_files)
    if parquet_files_count == 0:
        raise Exception("No parquet files were generated.")
    LOGGER.info(
        "%s parquet files were written in '%s'",
        parquet_files_count,
        output_path,
    )


def _create_snowflake_table_from_parquet(
    table_name: str, input_path: str, snow_connection: SnowConnection
) -> None:
    snow_connection.create_snowflake_table_from_local_parquet(table_name, input_path)


def _to_pandas(sampled_df: SparkDataFrame) -> pandas.DataFrame:
    LOGGER.debug("Converting Spark DataFrame to Pandas DataFrame")
    pandas_df = sampled_df.toPandas()
    for field in sampled_df.schema.fields:
        is_integer = field.dataType.typeName() in INTEGER_TYPE_COLLECTION
        is_spark_string = isinstance(field.dataType, SparkStringType)
        is_spark_binary = isinstance(field.dataType, SparkBinaryType)
        is_spark_timestamp = isinstance(field.dataType, SparkTimestampType)
        is_spark_float = isinstance(field.dataType, SparkFloatType)
        is_spark_boolean = isinstance(field.dataType, SparkBooleanType)
        is_spark_date = isinstance(field.dataType, SparkDateType)
        if is_integer:
            LOGGER.debug(
                "Converting Spark integer column '%s' to Pandas nullable '%s' type",
                field.name,
                PANDAS_LONG_TYPE,
            )
            pandas_df[field.name] = (
                pandas_df[field.name].astype(PANDAS_LONG_TYPE).fillna(0)
            )
        elif is_spark_string or is_spark_binary:
            LOGGER.debug(
                "Converting Spark string column '%s' to Pandas nullable '%s' type",
                field.name,
                PANDAS_STRING_TYPE,
            )
            pandas_df[field.name] = (
                pandas_df[field.name].astype(PANDAS_STRING_TYPE).fillna("")
            )
        elif is_spark_timestamp:
            LOGGER.debug(
                "Converting Spark timestamp column '%s' to UTC naive Pandas datetime",
                field.name,
            )
            pandas_df[field.name] = convert_all_to_utc_naive(
                pandas_df[field.name]
            ).fillna(pandas.NaT)
        elif is_spark_float:
            LOGGER.debug(
                "Converting Spark float column '%s' to Pandas nullable float",
                field.name,
            )
            pandas_df[field.name] = (
                pandas_df[field.name].astype(PANDAS_FLOAT_TYPE).fillna(0.0)
            )
        elif is_spark_boolean:
            LOGGER.debug(
                "Converting Spark boolean column '%s' to Pandas nullable boolean",
                field.name,
            )
            pandas_df[field.name] = (
                pandas_df[field.name].astype("boolean").fillna(False)
            )
        elif is_spark_date:
            LOGGER.debug(
                "Converting Spark date column '%s' to Pandas nullable datetime",
                field.name,
            )
            pandas_df[field.name] = pandas_df[field.name].fillna(pandas.NaT)

    return pandas_df


def convert_all_to_utc_naive(series: pandas.Series) -> pandas.Series:
    """Convert all timezone-aware or naive timestamps in a series to UTC naive.

    Naive timestamps are assumed to be in UTC and localized accordingly.
    Timezone-aware timestamps are converted to UTC and then made naive.

    Args:
        series (pandas.Series): A Pandas Series of `pd.Timestamp` objects,
            either naive or timezone-aware.

    Returns:
        pandas.Series: A Series of UTC-normalized naive timestamps (`tzinfo=None`).

    """

    def convert(ts):
        if ts.tz is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert("UTC").tz_localize(None)

    return series.apply(convert)

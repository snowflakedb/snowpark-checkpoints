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

import inspect
import json
import logging
import os
import re

from datetime import datetime
from typing import Any, Optional

import numpy as np

from pandera import DataFrameSchema

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, expr
from snowflake.snowpark.types import TimestampType
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.io_utils.io_file_manager import get_io_file_manager
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)
from snowflake.snowpark_checkpoints.utils.constants import (
    CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME,
    CHECKPOINT_TABLE_NAME_FORMAT,
    COLUMNS_KEY,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_EXECUTION_MODE,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    DEFAULT_KEY,
    FAIL_STATUS,
    PASS_STATUS,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
)
from snowflake.snowpark_checkpoints.utils.extra_config import (
    get_checkpoint_file,
)
from snowflake.snowpark_checkpoints.utils.pandera_check_manager import (
    PanderaCheckManager,
)
from snowflake.snowpark_checkpoints.utils.telemetry import STATUS_KEY, report_telemetry
from snowflake.snowpark_checkpoints.validation_result_metadata import (
    ValidationResultsMetadata,
)
from snowflake.snowpark_checkpoints.validation_results import ValidationResult


LOGGER = logging.getLogger(__name__)


def _replace_special_characters(checkpoint_name: str) -> str:
    """Replace special characters in the checkpoint name with underscores.

    Args:
        checkpoint_name (str): The checkpoint name to process.

    Returns:
        str: The checkpoint name with special characters replaced by underscores.

    """
    regex = r"^[a-zA-Z_\s-][a-zA-Z0-9$_\s-]*$"
    if not bool(re.match(regex, checkpoint_name)):
        raise ValueError(
            f"Invalid checkpoint name: {checkpoint_name}",
            "Checkpoint name must contain only alphanumeric characters, hyphens, underscores and dollar signs.",
        )
    return re.sub(r"[\s-]", "_", checkpoint_name)


def _process_sampling(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    job_context: Optional[SnowparkJobContext] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
):
    """Process a Snowpark DataFrame by sampling it according to the specified parameters.

    Adjusts the column casing of the provided Pandera schema to uppercase.

    Args:
        df (SnowparkDataFrame): The Snowpark DataFrame to be sampled.
        pandera_schema (DataFrameSchema): The Pandera schema to validate the DataFrame.
        job_context (SnowparkJobContext, optional): The job context for the sampling operation.
            Defaults to None.
        sample_frac (Optional[float], optional): The fraction of rows to sample.
            Defaults to 0.1.
        sample_number (Optional[int], optional): The number of rows to sample.
            Defaults to None.
        sampling_strategy (Optional[SamplingStrategy], optional): The strategy to use for sampling.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.

    Returns:
        Tuple[DataFrameSchema, pd.DataFrame]: A tuple containing the adjusted Pandera schema with uppercase column names
        and the sampled pandas DataFrame.

    """
    sampler = SamplingAdapter(
        job_context, sample_frac, sample_number, sampling_strategy
    )
    sampler.process_args([df])

    # fix up the column casing
    pandera_schema_upper = pandera_schema
    new_columns: dict[Any, Any] = {}

    for column in pandera_schema.columns:
        new_columns[column.upper()] = pandera_schema.columns[column]

    pandera_schema_upper = pandera_schema_upper.remove_columns(pandera_schema.columns)
    pandera_schema_upper = pandera_schema_upper.add_columns(new_columns)

    sample_df = sampler.get_sampled_pandas_args()[0]
    sample_df.index = np.ones(sample_df.count().iloc[0], dtype=int)

    return pandera_schema_upper, sample_df


def _generate_schema(
    checkpoint_name: str, output_path: Optional[str] = None
) -> DataFrameSchema:
    """Generate a DataFrameSchema based on the checkpoint name provided.

    This function reads a JSON file corresponding to the checkpoint name,
    extracts schema information, and constructs a DataFrameSchema object.
    It also adds custom checks for numeric and boolean types if specified
    in the JSON file.

    Args:
        checkpoint_name (str): The name of the checkpoint used to locate
                               the JSON file containing schema information.
        output_path (str): The path to the output directory.

        DataFrameSchema: A schema object representing the structure and
                         constraints of the DataFrame.
                         constraints of the DataFrame.

    """
    LOGGER.info(
        "Generating Pandera DataFrameSchema for checkpoint: '%s'", checkpoint_name
    )
    current_directory_path = (
        output_path if output_path else get_io_file_manager().getcwd()
    )

    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )

    if not get_io_file_manager().folder_exists(output_directory_path):
        raise ValueError(
            """Output directory snowpark-checkpoints-output does not exist.
Please run the Snowpark checkpoint collector first."""
        )

    checkpoint_schema_file_path = os.path.join(
        output_directory_path,
        CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME.format(checkpoint_name),
    )

    if not get_io_file_manager().file_exists(checkpoint_schema_file_path):
        raise ValueError(
            f"Checkpoint {checkpoint_name} JSON file not found. Please run the Snowpark checkpoint collector first."
        )

    LOGGER.info("Reading schema from file: '%s'", checkpoint_schema_file_path)
    schema_file = get_io_file_manager().read(checkpoint_schema_file_path)
    checkpoint_schema_config = json.loads(schema_file)

    if DATAFRAME_PANDERA_SCHEMA_KEY not in checkpoint_schema_config:
        raise ValueError(
            f"Pandera schema not found in the JSON file for checkpoint: {checkpoint_name}"
        )

    schema_dict = checkpoint_schema_config.get(DATAFRAME_PANDERA_SCHEMA_KEY)
    schema_dict_str = json.dumps(schema_dict)
    schema = DataFrameSchema.from_json(schema_dict_str)
    schema.coerce = False  # Disable coercion to ensure strict validation

    if DATAFRAME_CUSTOM_DATA_KEY not in checkpoint_schema_config:
        LOGGER.info(
            "No custom data found in the JSON file for checkpoint: '%s'",
            checkpoint_name,
        )
        return schema

    custom_data = checkpoint_schema_config.get(DATAFRAME_CUSTOM_DATA_KEY)

    if COLUMNS_KEY not in custom_data:
        raise ValueError(
            f"Columns not found in the JSON file for checkpoint: {checkpoint_name}"
        )

    pandera_check_manager = PanderaCheckManager(
        checkpoint_name=checkpoint_name, schema=schema
    )
    schema = pandera_check_manager.proccess_checks(custom_data)

    return schema


def _check_compare_data(
    df: SnowparkDataFrame,
    job_context: Optional[SnowparkJobContext],
    checkpoint_name: str,
    output_path: Optional[str] = None,
):
    """Compare the data in the provided Snowpark DataFrame with the data in a checkpoint table.

    This function writes the provided DataFrame to a table and compares it with an existing checkpoint table
    using a hash aggregation query. If there is a data mismatch, it marks the job context as failed and raises a
    SchemaValidationError. If the data matches, it marks the job context as passed.

    Args:
        df (SnowparkDataFrame): The Snowpark DataFrame to compare.
        job_context (Optional[SnowparkJobContext]): The job context containing the Snowpark session and job state.
        checkpoint_name (str): The name of the checkpoint table to compare against.
        output_path (Optional[str]): The path to the output directory.

    Raises:
        SchemaValidationError: If there is a data mismatch between the DataFrame and the checkpoint table.

    """
    _, err = _compare_data(df, job_context, checkpoint_name, output_path)
    if err is not None:
        raise err


@report_telemetry(
    params_list=["df"], return_indexes=[(STATUS_KEY, 0)], multiple_return=True
)
def _compare_data(
    df: SnowparkDataFrame,
    job_context: Optional[SnowparkJobContext],
    checkpoint_name: str,
    output_path: Optional[str] = None,
) -> tuple[bool, Optional[SchemaValidationError]]:
    """Compare the data in the provided Snowpark DataFrame with the data in a checkpoint table.

    This function writes the provided DataFrame to a table and compares it with an existing checkpoint table
    using a hash aggregation query. If there is a data mismatch, it marks the job context as failed and raises a
    SchemaValidationError. If the data matches, it marks the job context as passed.

    Args:
        df (SnowparkDataFrame): The Snowpark DataFrame to compare.
        job_context (Optional[SnowparkJobContext]): The job context containing the Snowpark session and job state.
        checkpoint_name (str): The name of the checkpoint table to compare against.
        output_path (Optional[str]): The path to the output directory.

    Returns:
        Tuple[bool, Optional[SchemaValidationError]]: A tuple containing a boolean indicating if the data matches
        and an optional SchemaValidationError if there is a data mismatch.

    Raises:
        SchemaValidationError: If there is a data mismatch between the DataFrame and the checkpoint table.

    """
    df = convert_timestamps_to_utc_date(df)
    new_table_name = CHECKPOINT_TABLE_NAME_FORMAT.format(checkpoint_name)
    LOGGER.info(
        "Writing Snowpark DataFrame to table: '%s' for checkpoint: '%s'",
        new_table_name,
        checkpoint_name,
    )
    df.write.save_as_table(table_name=new_table_name, mode="overwrite")

    LOGGER.info(
        "Comparing DataFrame to checkpoint table: '%s' for checkpoint: '%s'",
        new_table_name,
        checkpoint_name,
    )

    session = job_context.snowpark_session
    result = get_comparison_differences(session, checkpoint_name, new_table_name)
    has_failed = result.get("spark_only_rows") or result.get("snowpark_only_rows")
    if has_failed or result.get("error"):
        error_message = f"Data mismatch for checkpoint {checkpoint_name}: {result}"
        job_context._mark_fail(
            error_message,
            checkpoint_name,
            df,
            DATAFRAME_EXECUTION_MODE,
        )
        _update_validation_result(
            checkpoint_name,
            FAIL_STATUS,
            output_path,
        )
        return False, SchemaValidationError(
            error_message,
            job_context,
            checkpoint_name,
            df,
        )
    else:
        _update_validation_result(checkpoint_name, PASS_STATUS, output_path)
        job_context._mark_pass(checkpoint_name, DATAFRAME_EXECUTION_MODE)
        return True, None


def get_comparison_differences(
    session: Session, spark_table: str, snowpark_table: str
) -> dict:
    """Compare two tables and return the differences."""
    try:
        spark_raw_schema = session.table(spark_table).schema.names
        snowpark_raw_schema = session.table(snowpark_table).schema.names

        spark_normalized = {
            col_name.strip('"').upper(): col_name for col_name in spark_raw_schema
        }
        snowpark_normalized = {
            col_name.strip('"').upper(): col_name for col_name in snowpark_raw_schema
        }

        common_cols = sorted(
            list(
                set(spark_normalized.keys()).intersection(
                    set(snowpark_normalized.keys())
                )
            )
        )

        if not common_cols:
            return {
                "error": f"No common columns found between {spark_table} and {snowpark_table}",
            }

        cols_for_spark_selection = [
            spark_normalized[norm_col_name] for norm_col_name in common_cols
        ]
        cols_for_snowpark_selection = [
            snowpark_normalized[norm_col_name] for norm_col_name in common_cols
        ]

        spark_ordered = session.table(spark_table).select(
            *[col(c) for c in cols_for_spark_selection]
        )
        snowpark_ordered = session.table(snowpark_table).select(
            *[col(c) for c in cols_for_snowpark_selection]
        )

        spark_leftovers = spark_ordered.except_(snowpark_ordered).collect()
        snowpark_leftovers = snowpark_ordered.except_(spark_ordered).collect()

        spark_only_rows = [row.asDict() for row in spark_leftovers]
        snowpark_only_rows = [row.asDict() for row in snowpark_leftovers]

        return {
            "spark_only_rows": spark_only_rows,
            "snowpark_only_rows": snowpark_only_rows,
        }

    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}


def convert_timestamps_to_utc_date(df):
    """Convert and normalize all Snowpark timestamp columns to UTC.

    This function ensures timestamps are consistent across environments for reliable comparison.
    """
    new_cols = []
    for field in df.schema.fields:
        if isinstance(field.datatype, TimestampType):
            utc_midnight_ts = expr(
                f"convert_timezone('UTC', cast(to_date({field.name}) as timestamp_tz))"
            ).alias(field.name)
            new_cols.append(utc_midnight_ts)
        else:
            new_cols.append(col(field.name))
    return df.select(new_cols)


def _find_frame_in(stack: list[inspect.FrameInfo]) -> tuple:
    """Find a specific frame in the provided stack trace.

    This function searches through the provided stack trace to find a frame that matches
    certain criteria. It looks for frames where the function name is "wrapper" or where
    the code context matches specific regular expressions.

    Args:
        stack (list[inspect.FrameInfo]): A list of frame information objects representing
                                         the current stack trace.

    Returns:
        tuple: A tuple containing the relative path of the file and the line number of the
               matched frame. If no frame is matched, it returns a default key and -1.

    """
    regex = (
        r"(?<!_check_dataframe_schema_file)"
        r"(?<!_check_dataframe_schema)"
        r"(validate_dataframe_checkpoint|check_dataframe_schema)"
    )

    first_frames = stack[:7]
    first_frames.reverse()

    for i, frame in enumerate(first_frames):
        if frame.function == "wrapper" and i - 1 >= 0:
            next_frame = first_frames[i - 1]
            return _get_relative_path(next_frame.filename), next_frame.lineno

        if len(frame.code_context) >= 0 and re.search(regex, frame.code_context[0]):
            return _get_relative_path(frame.filename), frame.lineno
    return DEFAULT_KEY, -1


def _get_relative_path(file_path: str) -> str:
    """Get the relative path of a file.

    Args:
        file_path (str): The path to the file.

    Returns:
        str: The relative path of the file.

    """
    current_directory = get_io_file_manager().getcwd()
    return os.path.relpath(file_path, current_directory)


def _update_validation_result(
    checkpoint_name: str, validation_status: str, output_path: Optional[str] = None
) -> None:
    """Update the validation result file with the status of a given checkpoint.

    Args:
        checkpoint_name (str): The name of the checkpoint to update.
        validation_status (str): The validation status to record for the checkpoint.
        output_path (str): The path to the output directory.

    Returns:
        None

    """
    _file = get_checkpoint_file(checkpoint_name)

    stack = inspect.stack()

    _file_from_stack, _line_of_code = _find_frame_in(stack)

    pipeline_result_metadata = ValidationResultsMetadata(output_path)

    pipeline_result_metadata.clean()

    pipeline_result_metadata.add_validation_result(
        ValidationResult(
            timestamp=datetime.now().isoformat(),
            file=_file if _file else _file_from_stack,
            line_of_code=_line_of_code,
            checkpoint_name=checkpoint_name,
            result=validation_status,
        )
    )

    pipeline_result_metadata.save()

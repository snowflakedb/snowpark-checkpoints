#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import inspect
import json
import os
import re

from datetime import datetime
from typing import Any, Optional

import numpy as np

from pandera import Check, DataFrameSchema

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)
from snowflake.snowpark_checkpoints.utils.checkpoint_logger import CheckpointLogger
from snowflake.snowpark_checkpoints.utils.constant import (
    CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME,
    CHECKPOINT_TABLE_NAME_FORMAT,
    COLUMNS_KEY,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_EXECUTION_MODE,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    DECIMAL_PRECISION_KEY,
    DEFAULT_KEY,
    EXCEPT_HASH_AGG_QUERY,
    FAIL_STATUS,
    FALSE_COUNT_KEY,
    MARGIN_ERROR_KEY,
    MEAN_KEY,
    NAME_KEY,
    NULL_COUNT_KEY,
    NULLABLE_KEY,
    PASS_STATUS,
    ROWS_COUNT_KEY,
    SKIP_ALL,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    TRUE_COUNT_KEY,
    TYPE_KEY,
)
from snowflake.snowpark_checkpoints.utils.extra_config import (
    get_checkpoint_file,
)
from snowflake.snowpark_checkpoints.utils.supported_types import (
    BooleanTypes,
    NumericTypes,
)
from snowflake.snowpark_checkpoints.validation_result_metadata import (
    ValidationResultsMetadata,
)
from snowflake.snowpark_checkpoints.validation_results import ValidationResult


def _replace_special_characters(checkpoint_name: str) -> str:
    """Replace special characters in the checkpoint name with underscores.

    Args:
        checkpoint_name (str): The checkpoint name to process.

    Returns:
        str: The checkpoint name with special characters replaced by underscores.

    """
    return re.sub(r"\W", "_", checkpoint_name)


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

    for col in pandera_schema.columns:
        new_columns[col.upper()] = pandera_schema.columns[col]

    pandera_schema_upper = pandera_schema_upper.remove_columns(pandera_schema.columns)
    pandera_schema_upper = pandera_schema_upper.add_columns(new_columns)

    sample_df = sampler.get_sampled_pandas_args()[0]
    sample_df.index = np.ones(sample_df.count().iloc[0])

    return pandera_schema_upper, sample_df


def _add_numeric_checks(
    schema: DataFrameSchema, col: str, additional_check: dict[str, Any]
):
    """Add numeric checks to a specified column in a DataFrameSchema.

    Args:
        schema (DataFrameSchema): The schema to which the checks will be added.
        col (str): The name of the column to which the checks will be applied.
        additional_check (dict[str, Any]): A dictionary containing additional checks.
            - MEAN_KEY (str): The key for the mean value to check against.
            - MARGIN_ERROR_KEY (str): The key for the margin of error for the mean check.
            - DECIMAL_PRECISION_KEY (str): The key for the maximum decimal precision allowed.

    Returns:
        None

    """
    mean = additional_check.get(MEAN_KEY, 0)
    std = additional_check.get(MARGIN_ERROR_KEY, 0)

    def check_mean(series):
        series_mean = series.mean()
        return mean - std <= series_mean <= mean + std

    schema.columns[col].checks.append(
        Check(check_mean, element_wise=False, name="mean")
    )

    if DECIMAL_PRECISION_KEY in additional_check:
        schema.columns[col].checks.append(
            Check(
                lambda series: series.apply(
                    lambda x: len(str(x).split(".")[1]) if "." in str(x) else 0
                )
                <= additional_check[DECIMAL_PRECISION_KEY],
                name="decimal_precision",
            )
        )


def _add_boolean_checks(
    schema: DataFrameSchema, col: str, additional_check: dict[str, Any]
):
    """Add boolean checks to a specified column in a DataFrameSchema.

    Args:
        schema (DataFrameSchema): The schema to which the checks will be added.
        col (str): The name of the column to which the checks will be applied.
        additional_check (dict[str, Any]): A dictionary containing additional check parameters.
            - TRUE_COUNT_KEY (int): Expected count of True values in the column.
            - FALSE_COUNT_KEY (int): Expected count of False values in the column.
            - MARGIN_ERROR_KEY (int): Margin of error allowed for the counts.

    Returns:
        None

    """
    count_of_true = additional_check.get(TRUE_COUNT_KEY, 0)
    count_of_false = additional_check.get(FALSE_COUNT_KEY, 0)
    rows_count = additional_check.get(ROWS_COUNT_KEY, 0)
    std = additional_check.get(MARGIN_ERROR_KEY, 0)
    percentage_true = count_of_true / rows_count
    percentage_false = count_of_false / rows_count

    schema.columns[col].checks.extend(
        [
            Check(
                lambda series: (
                    percentage_true - std
                    <= series.value_counts().get(True, 0) / series.count()
                    if series.count() > 0
                    else 1 <= percentage_true + std
                ),
            ),
            Check(
                lambda series: (
                    percentage_false - std
                    <= series.value_counts().get(False, 0) / series.count()
                    if series.count() > 0
                    else 1 <= percentage_false + std
                ),
            ),
        ]
    )


def _add_null_checks(
    schema: DataFrameSchema, col: str, additional_check: dict[str, Any]
):
    """Add null checks to a specified column in a DataFrameSchema.

    Args:
        schema (DataFrameSchema): The schema to which the checks will be added.
        col (str): The name of the column to which the checks will be applied.
        additional_check (dict[str, Any]): A dictionary containing additional check parameters.
            - NULL_COUNT_KEY (int): Expected count of Null values in the column.
            - ROWS_COUNT_KEY (int): Total number of rows in the column.
            - MARGIN_ERROR_KEY (int): Margin of error allowed for the counts.

    Returns:
        None

    """
    count_of_null = additional_check.get(NULL_COUNT_KEY, 0)
    rows_count = additional_check.get(ROWS_COUNT_KEY, 0)
    std = additional_check.get(MARGIN_ERROR_KEY, 0)
    percentage_null = count_of_null / rows_count

    schema.columns[col].checks.extend(
        [
            Check(
                lambda series: (
                    percentage_null - std <= series.isnull().sum() / series.count()
                    if series.count() > 0
                    else 1 <= percentage_null + std
                ),
            ),
        ]
    )


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
    current_directory_path = output_path if output_path else os.getcwd()

    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )

    if not os.path.exists(output_directory_path):
        raise ValueError(
            """Output directory snowpark-checkpoints-output does not exist.
Please run the Snowpark checkpoint collector first."""
        )

    checkpoint_file_path = os.path.join(
        output_directory_path,
        CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME.format(checkpoint_name),
    )

    if not os.path.exists(checkpoint_file_path):
        raise ValueError(
            f"Checkpoint {checkpoint_name} JSON file not found. Please run the Snowpark checkpoint collector first."
        )

    with open(checkpoint_file_path) as custom_data_schema:
        custom_data_schema_json = json.load(custom_data_schema)

    if DATAFRAME_PANDERA_SCHEMA_KEY not in custom_data_schema_json:
        raise ValueError(
            f"Pandera schema not found in the JSON file for checkpoint: {checkpoint_name}"
        )

    schema_dict = custom_data_schema_json.get(DATAFRAME_PANDERA_SCHEMA_KEY)
    schema_dict_str = json.dumps(schema_dict)
    schema = DataFrameSchema.from_json(schema_dict_str)

    if DATAFRAME_CUSTOM_DATA_KEY not in custom_data_schema_json:
        return schema

    custom_data = custom_data_schema_json.get(DATAFRAME_CUSTOM_DATA_KEY)

    if COLUMNS_KEY not in custom_data:
        raise ValueError(
            f"Columns not found in the JSON file for checkpoint: {checkpoint_name}"
        )

    logger = CheckpointLogger().get_logger()

    for additional_check in custom_data.get(COLUMNS_KEY):

        type = additional_check.get(TYPE_KEY, None)
        name = additional_check.get(NAME_KEY, None)
        is_nullable = additional_check.get(NULLABLE_KEY, False)

        if name is None:
            raise ValueError(f"Column name not defined in the schema {checkpoint_name}")

        if type is None:
            raise ValueError(f"Type not defined for column {name}")

        if schema.columns.get(name) is None:
            logger.warning(f"Column {name} not found in schema")
            continue

        if type in NumericTypes:
            _add_numeric_checks(schema, name, additional_check)

        elif type in BooleanTypes:
            _add_boolean_checks(schema, name, additional_check)

        if is_nullable:
            _add_null_checks(schema, name, additional_check)

    return schema


def _skip_checks_on_schema(
    pandera_schema: DataFrameSchema,
    skip_checks: Optional[dict[str, list[str]]] = None,
) -> None:
    """Modify a Pandera DataFrameSchema to skip specified checks on certain columns.

    Args:
        pandera_schema (DataFrameSchema): The Pandera DataFrameSchema object to modify.
        skip_checks (Optional[dict[str, list[str]]]): A dictionary where keys are column names
                                                and values are lists of checks to skip for
                                                those columns. If the list is empty, all
                                                checks for the column will be removed.

    Returns:
        None

    """
    if not skip_checks:
        return

    for col, checks_to_skip in skip_checks.items():

        if col in pandera_schema.columns:

            if SKIP_ALL in checks_to_skip:
                pandera_schema.columns[col].checks = {}

            else:
                pandera_schema.columns[col].checks = [
                    check
                    for check in pandera_schema.columns[col].checks
                    if check.name not in checks_to_skip
                ]


def _add_custom_checks(
    schema: DataFrameSchema,
    custom_checks: Optional[dict[str, list[Check]]] = None,
):
    """Add custom checks to a Pandera DataFrameSchema.

    Args:
        schema (DataFrameSchema): The Pandera DataFrameSchema object to modify.
        custom_checks (Optional[dict[str, list[Check]]]): A dictionary where keys are column names
                                                and values are lists of checks to add for
                                                those columns.

    Returns:
        None

    """
    if not custom_checks:
        return

    for col, checks in custom_checks.items():
        if col in schema.columns:
            col_schema = schema.columns[col]
            col_schema.checks.extend(checks)
        else:
            raise ValueError(f"Column {col} not found in schema")


def _compare_data(
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
        job_context (SnowparkJobContext): The job context containing the Snowpark session and job state.
        checkpoint_name (str): The name of the checkpoint table to compare against.
        output_path (str): The path to the output directory.

    Raises:
        SchemaValidationError: If there is a data mismatch between the DataFrame and the checkpoint table.

    """
    new_table_name = CHECKPOINT_TABLE_NAME_FORMAT.format(checkpoint_name)

    df.write.save_as_table(table_name=new_table_name, mode="overwrite")

    expect_df = job_context.snowpark_session.sql(
        EXCEPT_HASH_AGG_QUERY, [checkpoint_name, new_table_name]
    )

    if expect_df.count() != 0:
        error_message = f"Data mismatch for checkpoint {checkpoint_name}"
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
        raise SchemaValidationError(
            error_message,
            job_context,
            checkpoint_name,
            df,
        )
    else:
        _update_validation_result(checkpoint_name, PASS_STATUS, output_path)
        job_context._mark_pass(checkpoint_name, DATAFRAME_EXECUTION_MODE)


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
    current_directory = os.getcwd()
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

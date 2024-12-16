#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
import os

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
from snowflake.snowpark_checkpoints.utils.constant import (
    CHECKPOINT_JSON_OUTPUT_DIRECTORY_ERROR,
    CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME,
    CHECKPOINT_JSON_OUTPUT_FILE_NOT_FOUND_ERROR,
    CHECKPOINT_TABLE_NAME_FORMAT,
    COLUMN_NAME_NOT_DEFINED_FORMAT_ERROR,
    COLUMN_NOT_FOUND_FORMAT_ERROR,
    COLUMNS_KEY,
    COLUMNS_NOT_FOUND_JSON_FORMAT_ERROR,
    CREATE_STAGE_STATEMENT_FORMAT,
    DATA_MISMATCH_ERROR,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    DECIMAL_PRECISION_KEY,
    EXCEPT_HASH_AGG_QUERY,
    FALSE_COUNT_KEY,
    MARGIN_ERROR_KEY,
    MEAN_KEY,
    NAME_KEY,
    PANDERA_NOT_FOUND_JSON_FORMAT_ERROR,
    SKIP_ALL,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    STAGE_NAME,
    TRUE_COUNT_KEY,
    TYPE_KEY,
    TYPE_NOT_DEFINED_FORMAT_ERROR,
)
from snowflake.snowpark_checkpoints.utils.supported_types import (
    BooleanTypes,
    NumericTypes,
)


def _process_sampling(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    job_context: Optional[SnowparkJobContext] = None,
    sample_frac: Optional[float] = 0.1,
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
    std = additional_check.get(MARGIN_ERROR_KEY, 0)

    schema.columns[col].checks.extend(
        [
            Check(
                lambda series: count_of_true - std
                <= series.value_counts().get(True, 0)
                <= count_of_true + std
            ),
            Check(
                lambda series: count_of_false - std
                <= series.value_counts().get(False, 0)
                <= count_of_false + std
            ),
        ]
    )


def _generate_schema(checkpoint_name: str) -> DataFrameSchema:
    """Generate a DataFrameSchema based on the checkpoint name provided.

    This function reads a JSON file corresponding to the checkpoint name,
    extracts schema information, and constructs a DataFrameSchema object.
    It also adds custom checks for numeric and boolean types if specified
    in the JSON file.

    Args:
        checkpoint_name (str): The name of the checkpoint used to locate
                               the JSON file containing schema information.

        DataFrameSchema: A schema object representing the structure and
                         constraints of the DataFrame.
                         constraints of the DataFrame.

    """
    current_directory_path = os.getcwd()

    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )

    if not os.path.exists(output_directory_path):
        raise ValueError(CHECKPOINT_JSON_OUTPUT_DIRECTORY_ERROR)

    checkpoint_file_path = os.path.join(
        output_directory_path,
        CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME.format(checkpoint_name),
    )

    if not os.path.exists(checkpoint_file_path):
        raise ValueError(
            CHECKPOINT_JSON_OUTPUT_FILE_NOT_FOUND_ERROR.format(checkpoint_name)
        )

    with open(checkpoint_file_path) as custom_data_schema:
        custom_data_schema_json = json.load(custom_data_schema)

    if DATAFRAME_PANDERA_SCHEMA_KEY not in custom_data_schema_json:
        raise ValueError(PANDERA_NOT_FOUND_JSON_FORMAT_ERROR.format(checkpoint_name))

    schema_dict = custom_data_schema_json.get(DATAFRAME_PANDERA_SCHEMA_KEY)
    schema_dict_str = json.dumps(schema_dict)
    schema = DataFrameSchema.from_json(schema_dict_str)

    if DATAFRAME_CUSTOM_DATA_KEY not in custom_data_schema_json:
        return schema

    custom_data = custom_data_schema_json.get(DATAFRAME_CUSTOM_DATA_KEY)

    if COLUMNS_KEY not in custom_data:
        raise ValueError(COLUMNS_NOT_FOUND_JSON_FORMAT_ERROR.format(checkpoint_name))

    for additional_check in custom_data.get(COLUMNS_KEY):

        type = additional_check.get(TYPE_KEY, None)
        name = additional_check.get(NAME_KEY, None)

        if name is None:
            raise ValueError(
                COLUMN_NAME_NOT_DEFINED_FORMAT_ERROR.format(checkpoint_name)
            )

        if type is None:
            raise ValueError(TYPE_NOT_DEFINED_FORMAT_ERROR.format(name))

        if type in NumericTypes:
            _add_numeric_checks(schema, name, additional_check)

        elif type in BooleanTypes:
            _add_boolean_checks(schema, name, additional_check)

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
            raise ValueError(COLUMN_NOT_FOUND_FORMAT_ERROR.format(col))


def _compare_data(
    df: SnowparkDataFrame,
    job_context: Optional[SnowparkJobContext],
    checkpoint_name: str,
):
    """Compare the data in the provided Snowpark DataFrame with the data in a checkpoint table.

    This function writes the provided DataFrame to a temporary table and compares it with an existing checkpoint table
    using a hash aggregation query. If there is a data mismatch, it marks the job context as failed and raises a
    SchemaValidationError. If the data matches, it marks the job context as passed.

    Args:
        df (SnowparkDataFrame): The Snowpark DataFrame to compare.
        job_context (SnowparkJobContext): The job context containing the Snowpark session and job state.
        checkpoint_name (str): The name of the checkpoint table to compare against.

    Raises:
        SchemaValidationError: If there is a data mismatch between the DataFrame and the checkpoint table.

    """
    new_table_name = CHECKPOINT_TABLE_NAME_FORMAT.format(checkpoint_name)

    _create_stage(job_context)

    df.write.save_as_table(table_name=new_table_name, mode="overwrite")

    expect_df = job_context.snowpark_session.sql(
        EXCEPT_HASH_AGG_QUERY, [checkpoint_name, new_table_name]
    )

    if expect_df.count() != 0:
        error_message = DATA_MISMATCH_ERROR.format(checkpoint_name)
        job_context.mark_fail(
            error_message,
            checkpoint_name,
            df,
        )
        raise SchemaValidationError(
            error_message,
            job_context,
            checkpoint_name,
            df,
        )
    else:
        job_context.mark_pass(checkpoint_name)


def _create_stage(job_context: Optional[SnowparkJobContext]) -> None:
    create_stage_statement = CREATE_STAGE_STATEMENT_FORMAT.format(STAGE_NAME)
    job_context.snowpark_session.sql(create_stage_statement).collect()

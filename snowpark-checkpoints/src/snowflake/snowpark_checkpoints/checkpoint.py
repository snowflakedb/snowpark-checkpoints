#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Wrapper around pandera which logs to snowflake
from typing import Any, Optional

import numpy as np
import pandas

from pandera import Check, DataFrameSchema
from pandera_report import DataFrameValidator

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)
from snowflake.snowpark_checkpoints.utils.constant import (
    COLUMN_NOT_FOUND_ERROR,
    SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR,
)
from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager
from snowflake.snowpark_checkpoints.utils.utils_checks import (
    generate_schema,
    skip_checks_on_schema,
)


def check_dataframe_schema_file(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    job_context: SnowparkJobContext = None,
    custom_checks: Optional[dict[Any, Any]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
):
    """Generate and checks the schema for a given DataFrame based on a checkpoint name.

    Args:
        df (SnowparkDataFrame): The DataFrame to be validated.
        checkpoint_name (str): The name of the checkpoint to retrieve the schema.
        custom_checks (dict[Any, Any], optional): Custom checks to be added to the schema.
            Defaults to None.
        skip_checks (dict[Any, Any], optional): Checks to be skipped.
            Defaults to None.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        sample_frac (float, optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_n (int, optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (SamplingStrategy, optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.

    Raises:
        SchemaValidationError: If the DataFrame fails schema validation.

    """
    schema = generate_schema(checkpoint_name)

    check_dataframe_schema(
        df,
        schema,
        job_context,
        checkpoint_name,
        custom_checks,
        skip_checks,
        sample_frac,
        sample_n,
        sampling_strategy,
    )


def check_dataframe_schema(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    job_context: SnowparkJobContext = None,
    checkpoint_name: str = None,
    custom_checks: Optional[dict[str, list[Check]]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
):
    """Validate a DataFrame against a given Pandera schema using sampling techniques.

    Args:
        df (SnowparkDataFrame): The DataFrame to be validated.
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        skip_checks (dict[Any, Any], optional): Checks to be skipped.
            Defaults to None.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        checkpoint_name (str, optional): The name of the checkpoint to retrieve the schema.
            Defaults to None.
        custom_checks (dict[Any, Any], optional): Custom checks to be added to the schema.
            Defaults to None.
        sample_frac (float, optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_n (int, optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (SamplingStrategy, optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.

    Raises:
        SchemaValidationError: If the DataFrame fails schema validation.

    """
    telemetry = TelemetryManager()
    skip_checks_on_schema(pandera_schema, skip_checks)

    if custom_checks:
        for col, checks in custom_checks.items():
            if col in pandera_schema.columns:
                col_schema = pandera_schema.columns[col]
                col_schema.checks.extend(checks)
            else:
                raise ValueError(COLUMN_NOT_FOUND_ERROR.format(col))

    sampler = SamplingAdapter(job_context, sample_frac, sample_n, sampling_strategy)
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
    # Raises SchemaError on validation issues
    try:
        validator = DataFrameValidator()
        validation, result = validator.validate(
            pandera_schema_upper, sample_df, validity_flag=True
        )

        telemetry.log_info(
            "DataFrame_Validator",
            {
                "type": "check_dataframe_schema",
                "status": result,
            },
        )

        if job_context is not None:
            job_context.mark_pass(checkpoint_name)

        return validation
    except Exception as pandera_ex:
        telemetry_data = {
            "type": "check_dataframe_schema",
            "error": "SnowparkOutputSchemaValidationError",
        }
        telemetry.log_error("DataFrame_Validator_Error", telemetry_data)
        raise SchemaValidationError(
            SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR,
            job_context,
            checkpoint_name,
            pandera_ex,
        ) from pandera_ex


def check_output_schema(
    pandera_schema: DataFrameSchema,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: SnowparkJobContext = None,
    check_name: Optional[str] = None,
):
    """Decorate to validate the schema of the output of a Snowpark function.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        sample_frac (Optional[float], optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_n (Optional[int], optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (Optional[SamplingStrategy], optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        check_name (Optional[str], optional): The name of the checkpoint to retrieve the schema.
            Defaults to None.

    """

    def check_output_with_decorator(snowpark_fn):
        """Decorate to validate the schema of the output of a Snowpark function.

        Args:
            snowpark_fn (function): The Snowpark function to validate.

        Returns:
            function: The decorated function.

        """
        telemetry = TelemetryManager()
        checkpoint_name = check_name
        if check_name is None:
            checkpoint_name = snowpark_fn.__name__

        def wrapper(*args, **kwargs):
            """Wrapp a function to validate the schema of the output of a Snowpark function.

            Args:
                *args: The arguments to the Snowpark function.
                **kwargs: The keyword arguments to the Snowpark function.

            Returns:
                Any: The result of the Snowpark function.

            """
            # Run the sampled data in snowpark
            snowpark_results = snowpark_fn(*args, **kwargs)
            sampler = SamplingAdapter(
                job_context, sample_frac, sample_n, sampling_strategy
            )
            sampler.process_args([snowpark_results])
            pandas_sample_args = sampler.get_sampled_pandas_args()

            # Raises SchemaError on validation issues
            try:
                validator = DataFrameValidator()
                validation, result = validator.validate(
                    pandera_schema, pandas_sample_args[0], validity_flag=True
                )

                telemetry.log_info(
                    "DataFrame_Validator",
                    {
                        "type": "check_output_schema",
                        "status": result,
                    },
                )
                if job_context is not None:
                    job_context.mark_pass(checkpoint_name)

                print(validation)
            except Exception as pandera_ex:
                telemetry_data = {
                    "type": "check_output_schema",
                    "error": "SnowparkOutputSchemaValidationError",
                }
                telemetry.log_error("DataFrame_Validator_Error", telemetry_data)
                raise SchemaValidationError(
                    SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR,
                    job_context,
                    checkpoint_name,
                    pandera_ex,
                ) from pandera_ex
            return snowpark_results

        return wrapper

    return check_output_with_decorator


def check_input_schema(
    pandera_schema: DataFrameSchema,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: SnowparkJobContext = None,
    check_name: Optional[str] = None,
):
    """Decorate factory for validating input DataFrame schemas before function execution.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        sample_frac (Optional[float], optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_n (Optional[int], optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (Optional[SamplingStrategy], optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        check_name (Optional[str], optional): The name of the checkpoint to retrieve the schema.
            Defaults to None.

    """

    def check_input_with_decorator(snowpark_fn):
        """Decorate that validates input schemas for the decorated function.

        Args:
            snowpark_fn (Callable): The function to be decorated with input schema validation.

        Raises:
            SchemaValidationError: If input data fails schema validation.

        Returns:
            Callable: A wrapper function that performs schema validation before executing the original function.

        """
        telemetry = TelemetryManager()
        checkpoint_name = check_name
        if check_name is None:
            checkpoint_name = snowpark_fn.__name__

        def wrapper(*args, **kwargs):
            """Wrapp a function to validate the schema of the input of a Snowpark function.

            Raises:
                SchemaValidationError: If any input DataFrame fails schema validation.

            Returns:
                Any: The result of the original function after input validation.

            """
            # Run the sampled data in snowpark
            sampler = SamplingAdapter(
                job_context, sample_frac, sample_n, sampling_strategy
            )
            sampler.process_args(args)
            pandas_sample_args = sampler.get_sampled_pandas_args()

            # Raises SchemaError on validation issues
            for arg in pandas_sample_args:
                if isinstance(arg, pandas.DataFrame):
                    try:
                        validator = DataFrameValidator()
                        validation, result = validator.validate(
                            pandera_schema, arg, validity_flag=True
                        )

                        if job_context is not None:
                            job_context.mark_pass(checkpoint_name)

                        telemetry.log_info(
                            "DataFrame_Validator",
                            {
                                "type": "check_input_schema",
                                "status": result,
                            },
                        )

                        print(validation)
                    except Exception as pandera_ex:
                        telemetry_data = {
                            "type": "check_input_schema",
                            "error": "SnowparkOutputSchemaValidationError",
                        }
                        telemetry.log_error("DataFrame_Validator_Error", telemetry_data)
                        raise SchemaValidationError(
                            SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR,
                            job_context,
                            checkpoint_name,
                            pandera_ex,
                        ) from pandera_ex
            return snowpark_fn(*args, **kwargs)

        return wrapper

    return check_input_with_decorator

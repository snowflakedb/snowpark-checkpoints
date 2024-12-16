#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Wrapper around pandera which logs to snowflake
from typing import Any, Optional, Union

import numpy as np

from pandas import DataFrame as PandasDataFrame
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
    CHECKPOINT_NAME_IS_REQUIRED_ERROR,
    DATA_FRAME_IS_REQUIRED_ERROR,
    INVALID_VALIDATION_MODE_ERROR,
    SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR,
    CheckpointMode,
)
from snowflake.snowpark_checkpoints.utils.extra_config import is_checkpoint_enabled
from snowflake.snowpark_checkpoints.utils.utils_checks import (
    _add_custom_checks,
    _compare_data,
    _generate_schema,
    _process_sampling,
    _skip_checks_on_schema,
)


def validate_dataframe_checkpoint(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    mode: Optional[CheckpointMode] = CheckpointMode.SCHEMA,
    job_context: Optional[SnowparkJobContext] = None,
    custom_checks: Optional[dict[Any, Any]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 0.1,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
) -> Union[tuple[bool, PandasDataFrame], None]:
    """Validate a Snowpark DataFrame against a specified checkpoint.

    Args:
        df (SnowparkDataFrame): The DataFrame to validate.
        checkpoint_name (str): The name of the checkpoint to validate against.
        mode (CheckpointMode): The mode of validation (e.g., SCHEMA, PARQUET). Defaults to SCHEMA.
        job_context (SnowparkJobContext, optional): The job context for the validation. Required for PARQUET mode.
        custom_checks (Optional[dict[Any, Any]], optional): Custom checks to apply during validation.
        skip_checks (Optional[dict[Any, Any]], optional): Checks to skip during validation.
        sample_frac (Optional[float], optional): Fraction of the DataFrame to sample for validation. Defaults to 0.1.
        sample_number (Optional[int], optional): Number of rows to sample for validation.
        sampling_strategy (Optional[SamplingStrategy], optional): Strategy to use for sampling.
            Defaults to RANDOM_SAMPLE.

    Returns:
        Union[tuple[bool, PandasDataFrame], None]: A tuple containing a boolean indicating success
        and a Pandas DataFrame with validation results, or None if validation is not applicable.

    Raises:
        ValueError: If an invalid validation mode is provided or if job_context is None for PARQUET mode.

    """
    if mode == CheckpointMode.SCHEMA:
        return _check_dataframe_schema_file(
            df,
            checkpoint_name,
            job_context,
            custom_checks,
            skip_checks,
            sample_frac,
            sample_number,
            sampling_strategy,
        )
    elif mode == CheckpointMode.DATAFRAME:
        if job_context is None:
            raise ValueError(
                "Connectionless mode is not supported for Parquet validation"
            )
        _compare_data(df, job_context, checkpoint_name)
    else:
        raise ValueError(INVALID_VALIDATION_MODE_ERROR)


def _check_dataframe_schema_file(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    job_context: Optional[SnowparkJobContext] = None,
    custom_checks: Optional[dict[Any, Any]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 0.1,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
) -> tuple[bool, PandasDataFrame]:
    """Generate and checks the schema for a given DataFrame based on a checkpoint name.

    Args:
        df (SnowparkDataFrame): The DataFrame to be validated.
        checkpoint_name (str): The name of the checkpoint to retrieve the schema.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        custom_checks (dict[Any, Any], optional): Custom checks to be added to the schema.
            Defaults to None.
        skip_checks (dict[Any, Any], optional): Checks to be skipped.
            Defaults to None.
        sample_frac (float, optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_number (int, optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (SamplingStrategy, optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.

    Raises:
        SchemaValidationError: If the DataFrame fails schema validation.

    Returns:
        tuple[bool, PanderaDataFrame]: A tuple containing the validity flag and the Pandera DataFrame.

    """
    if df is None:
        raise ValueError(DATA_FRAME_IS_REQUIRED_ERROR)

    if checkpoint_name is None:
        raise ValueError(CHECKPOINT_NAME_IS_REQUIRED_ERROR)

    schema = _generate_schema(checkpoint_name)

    return check_dataframe_schema(
        df,
        schema,
        job_context,
        checkpoint_name,
        custom_checks,
        skip_checks,
        sample_frac,
        sample_number,
        sampling_strategy,
    )


def check_dataframe_schema(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    job_context: Optional[SnowparkJobContext] = None,
    checkpoint_name: str = None,
    custom_checks: Optional[dict[str, list[Check]]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 0.1,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
) -> Union[tuple[bool, PandasDataFrame], None]:
    """Validate a DataFrame against a given Pandera schema using sampling techniques.

    Args:
        df (SnowparkDataFrame): The DataFrame to be validated.
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        checkpoint_name (str, optional): The name of the checkpoint to retrieve the schema.
            Defaults to None.
        custom_checks (dict[Any, Any], optional): Custom checks to be added to the schema.
            Defaults to None.
        skip_checks (dict[Any, Any], optional): Checks to be skipped.
            Defaults to None.
        sample_frac (float, optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_number (int, optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (SamplingStrategy, optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.

    Raises:
        SchemaValidationError: If the DataFrame fails schema validation.

    Returns:
        Union[tuple[bool, PandasDataFrame]|None]: A tuple containing the validity flag and the Pandas DataFrame.
        If the validation for that checkpoint is disabled it returns None.

    """
    if df is None:
        raise ValueError("DataFrame is required")

    if pandera_schema is None:
        raise ValueError("Schema is required")

    if is_checkpoint_enabled(checkpoint_name):
        return _check_dataframe_schema(
            df,
            pandera_schema,
            job_context,
            checkpoint_name,
            custom_checks,
            skip_checks,
            sample_frac,
            sample_n,
            sampling_strategy,
        )


def _check_dataframe_schema(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    job_context: SnowparkJobContext = None,
    checkpoint_name: str = None,
    custom_checks: Optional[dict[str, list[Check]]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
) -> tuple[bool, PandasDataFrame]:

    _skip_checks_on_schema(pandera_schema, skip_checks)

    _add_custom_checks(pandera_schema, custom_checks)

    pandera_schema_upper, sample_df = _process_sampling(
        df, pandera_schema, job_context, sample_frac, sample_number, sampling_strategy
    )

    # Raises SchemaError on validation issues
    try:
        validator = DataFrameValidator()
        validation_result = validator.validate(
            pandera_schema_upper, sample_df, validity_flag=True
        )

        if job_context is not None:
            job_context.mark_pass(checkpoint_name)

        return validation_result
    except Exception as pandera_ex:
        raise SchemaValidationError(
            SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR,
            job_context,
            checkpoint_name,
            pandera_ex,
        ) from pandera_ex


def check_output_schema(
    pandera_schema: DataFrameSchema,
    sample_frac: Optional[float] = 0.1,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
    check_name: Optional[str] = None,
):
    """Decorate to validate the schema of the output of a Snowpark function.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        sample_frac (Optional[float], optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_number (Optional[int], optional): Number of rows to sample.
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
                job_context, sample_frac, sample_number, sampling_strategy
            )
            sampler.process_args([snowpark_results])
            pandas_sample_args = sampler.get_sampled_pandas_args()

            # Raises SchemaError on validation issues
            try:
                validator = DataFrameValidator()
                validation_result = validator.validate(
                    pandera_schema, pandas_sample_args[0], validity_flag=True
                )

                if job_context is not None:
                    job_context.mark_pass(checkpoint_name)

                print(validation_result)
            except Exception as pandera_ex:
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
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
    check_name: Optional[str] = None,
):
    """Decorate factory for validating input DataFrame schemas before function execution.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        sample_frac (Optional[float], optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_number (Optional[int], optional): Number of rows to sample.
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
                job_context, sample_frac, sample_number, sampling_strategy
            )
            sampler.process_args(args)
            pandas_sample_args = sampler.get_sampled_pandas_args()

            # Raises SchemaError on validation issues
            for arg in pandas_sample_args:
                if isinstance(arg, PandasDataFrame):
                    try:
                        validator = DataFrameValidator()
                        validation_result = validator.validate(
                            pandera_schema, arg, validity_flag=True
                        )

                        if job_context is not None:
                            job_context.mark_pass(checkpoint_name)

                        print(validation_result)
                    except Exception as pandera_ex:
                        raise SchemaValidationError(
                            SNOWPARK_OUTPUT_SCHEMA_VALIDATOR_ERROR,
                            job_context,
                            checkpoint_name,
                            pandera_ex,
                        ) from pandera_ex
            return snowpark_fn(*args, **kwargs)

        return wrapper

    return check_input_with_decorator

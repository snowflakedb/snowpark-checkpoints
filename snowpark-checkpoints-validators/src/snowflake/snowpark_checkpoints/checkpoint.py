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

# Wrapper around pandera which logs to snowflake
from typing import Any, Optional, Union, cast

from pandas import DataFrame as PandasDataFrame
from pandera import Check, DataFrameModel, DataFrameSchema
from pandera.errors import SchemaError, SchemaErrors

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)
from snowflake.snowpark_checkpoints.utils.checkpoint_logger import CheckpointLogger
from snowflake.snowpark_checkpoints.utils.constants import (
    FAIL_STATUS,
    PASS_STATUS,
    CheckpointMode,
)
from snowflake.snowpark_checkpoints.utils.extra_config import is_checkpoint_enabled
from snowflake.snowpark_checkpoints.utils.pandera_check_manager import (
    PanderaCheckManager,
)
from snowflake.snowpark_checkpoints.utils.telemetry import STATUS_KEY, report_telemetry
from snowflake.snowpark_checkpoints.utils.utils_checks import (
    _check_compare_data,
    _generate_schema,
    _process_sampling,
    _replace_special_characters,
    _update_validation_result,
)


def validate_dataframe_checkpoint(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    job_context: Optional[SnowparkJobContext] = None,
    mode: Optional[CheckpointMode] = CheckpointMode.SCHEMA,
    custom_checks: Optional[dict[Any, Any]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    output_path: Optional[str] = None,
) -> Union[tuple[bool, PandasDataFrame], None]:
    """Validate a Snowpark DataFrame against a specified checkpoint.

    Args:
        df (SnowparkDataFrame): The DataFrame to validate.
        checkpoint_name (str): The name of the checkpoint to validate against.
        job_context (SnowparkJobContext, optional): The job context for the validation. Required for PARQUET mode.
        mode (CheckpointMode): The mode of validation (e.g., SCHEMA, PARQUET). Defaults to SCHEMA.
        custom_checks (Optional[dict[Any, Any]], optional): Custom checks to apply during validation.
        skip_checks (Optional[dict[Any, Any]], optional): Checks to skip during validation.
        sample_frac (Optional[float], optional): Fraction of the DataFrame to sample for validation. Defaults to 0.1.
        sample_number (Optional[int], optional): Number of rows to sample for validation.
        sampling_strategy (Optional[SamplingStrategy], optional): Strategy to use for sampling.
            Defaults to RANDOM_SAMPLE.
        output_path (Optional[str], optional): The output path for the validation results.

    Returns:
        Union[tuple[bool, PandasDataFrame], None]: A tuple containing a boolean indicating success
        and a Pandas DataFrame with validation results, or None if validation is not applicable.

    Raises:
        ValueError: If an invalid validation mode is provided or if job_context is None for PARQUET mode.

    """
    checkpoint_name = _replace_special_characters(checkpoint_name)

    if is_checkpoint_enabled(checkpoint_name):

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
                output_path,
            )
        elif mode == CheckpointMode.DATAFRAME:
            if job_context is None:
                raise ValueError(
                    "Connectionless mode is not supported for Parquet validation"
                )
            _check_compare_data(df, job_context, checkpoint_name, output_path)
        else:
            raise ValueError(
                """Invalid validation mode.
                Please use for schema validation use a 1 or for a full data validation use a 2 for schema validation."""
            )


def _check_dataframe_schema_file(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    job_context: Optional[SnowparkJobContext] = None,
    custom_checks: Optional[dict[Any, Any]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    output_path: Optional[str] = None,
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
        output_path (str, optional): The output path for the validation results.

    Raises:
        SchemaValidationError: If the DataFrame fails schema validation.

    Returns:
        tuple[bool, PanderaDataFrame]: A tuple containing the validity flag and the Pandera DataFrame.

    """
    if df is None:
        raise ValueError("DataFrame is required")

    if checkpoint_name is None:
        raise ValueError("Checkpoint name is required")

    schema = _generate_schema(checkpoint_name, output_path)

    return check_dataframe_schema(
        df,
        schema,
        checkpoint_name,
        job_context,
        custom_checks,
        skip_checks,
        sample_frac,
        sample_number,
        sampling_strategy,
        output_path,
    )


def check_dataframe_schema(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    job_context: Optional[SnowparkJobContext] = None,
    custom_checks: Optional[dict[str, list[Check]]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    output_path: Optional[str] = None,
) -> Union[tuple[bool, PandasDataFrame], None]:
    """Validate a DataFrame against a given Pandera schema using sampling techniques.

    Args:
        df (SnowparkDataFrame): The DataFrame to be validated.
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        checkpoint_name (str, optional): The name of the checkpoint to retrieve the schema.
            Defaults to None.
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
        output_path (str, optional): The output path for the validation results.

    Raises:
        SchemaValidationError: If the DataFrame fails schema validation.

    Returns:
        Union[tuple[bool, PandasDataFrame]|None]: A tuple containing the validity flag and the Pandas DataFrame.
        If the validation for that checkpoint is disabled it returns None.

    """
    checkpoint_name = _replace_special_characters(checkpoint_name)

    if df is None:
        raise ValueError("DataFrame is required")

    if pandera_schema is None:
        raise ValueError("Schema is required")

    if is_checkpoint_enabled(checkpoint_name):
        return _check_dataframe_schema(
            df,
            pandera_schema,
            checkpoint_name,
            job_context,
            custom_checks,
            skip_checks,
            sample_frac,
            sample_number,
            sampling_strategy,
            output_path,
        )


@report_telemetry(
    params_list=["pandera_schema"],
    return_indexes=[(STATUS_KEY, 0)],
    multiple_return=True,
)
def _check_dataframe_schema(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    job_context: SnowparkJobContext = None,
    custom_checks: Optional[dict[str, list[Check]]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    output_path: Optional[str] = None,
) -> tuple[bool, PandasDataFrame]:

    pandera_check_manager = PanderaCheckManager(checkpoint_name, pandera_schema)
    pandera_check_manager.skip_checks_on_schema(skip_checks)
    pandera_check_manager.add_custom_checks(custom_checks)

    pandera_schema_upper, sample_df = _process_sampling(
        df, pandera_schema, job_context, sample_frac, sample_number, sampling_strategy
    )
    is_valid, validation_result = _validate(pandera_schema_upper, sample_df)
    if is_valid:
        if job_context is not None:
            job_context._mark_pass(checkpoint_name)
        _update_validation_result(checkpoint_name, PASS_STATUS, output_path)
    else:
        _update_validation_result(checkpoint_name, FAIL_STATUS, output_path)
        raise SchemaValidationError(
            "Snowpark DataFrame schema validation error",
            job_context,
            checkpoint_name,
            validation_result,
        )

    return (is_valid, validation_result)


@report_telemetry(params_list=["pandera_schema"])
def check_output_schema(
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
    output_path: Optional[str] = None,
):
    """Decorate to validate the schema of the output of a Snowpark function.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        checkpoint_name (Optional[str], optional): The name of the checkpoint to retrieve the schema.
        sample_frac (Optional[float], optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_number (Optional[int], optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (Optional[SamplingStrategy], optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        output_path (Optional[str], optional): The output path for the validation results.

    """

    def check_output_with_decorator(snowpark_fn):
        """Decorate to validate the schema of the output of a Snowpark function.

        Args:
            snowpark_fn (function): The Snowpark function to validate.

        Returns:
            function: The decorated function.

        """
        _checkpoint_name = checkpoint_name
        if checkpoint_name is None:
            _checkpoint_name = snowpark_fn.__name__
        _checkpoint_name = _replace_special_characters(_checkpoint_name)

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

            is_valid, validation_result = _validate(
                pandera_schema, pandas_sample_args[0]
            )
            logger = CheckpointLogger().get_logger()
            logger.info(
                f"Checkpoint {_checkpoint_name} validation result:\n{validation_result}"
            )

            if is_valid:
                if job_context is not None:
                    job_context._mark_pass(_checkpoint_name)

                _update_validation_result(_checkpoint_name, PASS_STATUS, output_path)
            else:
                _update_validation_result(_checkpoint_name, FAIL_STATUS, output_path)
                raise SchemaValidationError(
                    "Snowpark output schema validation error",
                    job_context,
                    _checkpoint_name,
                    validation_result,
                )

            return snowpark_results

        return wrapper

    return check_output_with_decorator


@report_telemetry(params_list=["pandera_schema"])
def check_input_schema(
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
    output_path: Optional[str] = None,
):
    """Decorate factory for validating input DataFrame schemas before function execution.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        checkpoint_name (Optional[str], optional): The name of the checkpoint to retrieve the schema.
        sample_frac (Optional[float], optional): Fraction of data to sample.
            Defaults to 0.1.
        sample_number (Optional[int], optional): Number of rows to sample.
            Defaults to None.
        sampling_strategy (Optional[SamplingStrategy], optional): Strategy for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.
        job_context (SnowparkJobContext, optional): Context for job-related operations.
            Defaults to None.
        output_path (Optional[str], optional): The output path for the validation results.


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
        _checkpoint_name = checkpoint_name
        if checkpoint_name is None:
            _checkpoint_name = snowpark_fn.__name__
        _checkpoint_name = _replace_special_characters(_checkpoint_name)

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

                    is_valid, validation_result = _validate(
                        pandera_schema,
                        arg,
                    )

                    logger = CheckpointLogger().get_logger()
                    logger.info(
                        f"Checkpoint {checkpoint_name} validation result:\n{validation_result}"
                    )

                    if is_valid:
                        if job_context is not None:
                            job_context._mark_pass(
                                _checkpoint_name,
                            )

                        _update_validation_result(
                            _checkpoint_name,
                            PASS_STATUS,
                            output_path,
                        )
                    else:
                        _update_validation_result(
                            _checkpoint_name,
                            FAIL_STATUS,
                            output_path,
                        )
                        raise SchemaValidationError(
                            "Snowpark input schema validation error",
                            job_context,
                            _checkpoint_name,
                            validation_result,
                        )
            return snowpark_fn(*args, **kwargs)

        return wrapper

    return check_input_with_decorator


def _validate(
    schema: Union[type[DataFrameModel], DataFrameSchema],
    df: PandasDataFrame,
    lazy: bool = True,
) -> tuple[bool, PandasDataFrame]:
    if not isinstance(schema, DataFrameSchema):
        schema = schema.to_schema()
    is_valid = True
    try:
        df = schema.validate(df, lazy=lazy)
    except (SchemaErrors, SchemaError) as schema_errors:
        df = cast(PandasDataFrame, schema_errors.failure_cases)
        is_valid = False
    return is_valid, df

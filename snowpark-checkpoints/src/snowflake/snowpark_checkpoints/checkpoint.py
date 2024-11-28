#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Wrapper around pandera which logs to snowflake
from typing import Any, Dict, Optional
from pandera import DataFrameSchema
import numpy as np
import pandas
from pandera_report import DataFrameValidator
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from .utils import TelemetryManager
from .utils.utils_checks import generate_schema


def check_df_schema_file(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    job_context: SnowparkJobContext = None,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
):
    """
    Validates the schema of a Snowpark DataFrame against a predefined Pandera schema.

    Args:
        df (SnowparkDataFrame): The Snowpark DataFrame to be validated.
        checkpoint_name (str): The name of the checkpoint to retrieve the schema.
        job_context (SnowparkJobContext, optional): The job context for the Snowpark job. Defaults to None.
        sample (Optional[float], optional): The fraction of the DataFrame to sample for validation. Defaults to 0.1.
        sampling_strategy (Optional[SamplingStrategy], optional): The strategy to use for sampling. Defaults to SamplingStrategy.RANDOM_SAMPLE.

    Returns:
        None
    """
    schema = generate_schema(checkpoint_name)

    check_df_schema(
        df,
        schema,
        job_context,
        checkpoint_name,
        sample_frac,
        sample_n,
        sampling_strategy,
    )


def check_df_schema(
    df: SnowparkDataFrame,
    pandera_schema: DataFrameSchema,
    job_context: SnowparkJobContext = None,
    checkpoint_name: str = None,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
):
    """
    Validates the schema of a Snowpark DataFrame against a predefined Pandera schema.

    Args:
        df (SnowparkDataFrame): The Snowpark DataFrame to be validated.
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        job_context (SnowparkJobContext, optional): The job context for the Snowpark job. Defaults to None.
        checkpoint_name (str, optional): The name of the checkpoint to retrieve the schema. Defaults to None.
        sample (Optional[float], optional): The fraction of the DataFrame to sample for validation. Defaults to 1.
        sampling_strategy (Optional[SamplingStrategy], optional): The strategy to use for sampling. Defaults to SamplingStrategy.RANDOM_SAMPLE.

    Returns:

    """
    sampler = SamplingAdapter(job_context, sample_frac, sample_n, sampling_strategy)
    sampler.process_args([df])

    # fix up the column casing
    pandera_schema_upper = pandera_schema
    new_columns: Dict[Any, Any] = {}

    # this can be updated the column names to be upper case
    # data.columns = map(str.lower, data.columns)
    for col in pandera_schema.columns:
        new_columns[col.upper()] = pandera_schema.columns[col]

    pandera_schema_upper = pandera_schema_upper.remove_columns(pandera_schema.columns)
    pandera_schema_upper = pandera_schema_upper.add_columns(new_columns)

    sample_df = sampler.get_sampled_pandas_args()[0]
    sample_df.index = np.ones(sample_df.count().iloc[0])
    telemetry = TelemetryManager()
    # Raises SchemaError on validation issues
    try:
        validator = DataFrameValidator()
        is_valid, result = validator.validate(
            pandera_schema_upper, sample_df, validity_flag=True
        )
        if job_context is not None:
            job_context.mark_pass(checkpoint_name)
            telemetry_data = {
                "validation": is_valid,
                "schema": [str(type) for type in result.dtypes.values],
            }
            telemetry.log_info("DataFrameSchemaValidator", telemetry_data)

    except Exception as pandera_ex:
        telemetry_data = {
            "error": "Snowpark output schema validation error",
            "pandera_ex": pandera_ex.message,
        }
        telemetry.log_error("DataFrameValidator_Error", telemetry_data)
        raise SchemaValidationError(
            "Snowpark output schema validation error",
            job_context,
            checkpoint_name,
            pandera_ex,
        )


def check_output_schema(
    pandera_schema: DataFrameSchema,
    sample_frac: Optional[float] = 0.1,
    sample_n: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: SnowparkJobContext = None,
    check_name: Optional[str] = None,
):
    """
    Decorator to validate the schema of the output of a Snowpark function.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        sample (Optional[int], optional): The number of rows to sample for validation. Defaults to 100.
        sampling_strategy (Optional[SamplingStrategy], optional): The strategy to use for sampling. Defaults to SamplingStrategy.RANDOM_SAMPLE.
        job_context (SnowparkJobContext, optional): The job context for the Snowpark job. Defaults to None.
        check_name (Optional[str], optional): The name of the checkpoint to retrieve the schema. Defaults to None.

    Returns:
        function: The decorated function.
    """

    def check_output_with_decorator(snowpark_fn):
        """
        Decorator to validate the schema of the output of a Snowpark function.

        Args:
            snowpark_fn (function): The Snowpark function to validate.

        Returns:
            function: The decorated function.
        """
        checkpoint_name = check_name
        if check_name is None:
            checkpoint_name = snowpark_fn.__name__

        def wrapper(*args, **kwargs):
            """
            Wrapper function to validate the schema of the output of a Snowpark function.

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
            telemetry = TelemetryManager()
            # Raises SchemaError on validation issues
            try:
                validator = DataFrameValidator()
                is_valid, result = validator.validate(
                    pandera_schema, pandas_sample_args[0], validity_flag=True
                )
                if job_context is not None:
                    telemetry_data = {
                        "validation": is_valid,
                        "schema": [str(type) for type in result.dtypes.values],
                    }
                    telemetry.log_info("DataFrameSchemaValidator", telemetry_data)
                    job_context.mark_pass(checkpoint_name)
            except Exception as pandera_ex:
                telemetry_data = {
                    "error": "Snowpark output schema validation error",
                    "pandera_ex": pandera_ex.message,
                }
                telemetry.log_error("DataFrameValidator_Error", telemetry_data)
                raise SchemaValidationError(
                    "Snowpark output schema validation error",
                    job_context,
                    checkpoint_name,
                    pandera_ex,
                )
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
    """
    Decorator to validate the schema of the input of a Snowpark function.

    Args:
        pandera_schema (DataFrameSchema): The Pandera schema to validate against.
        sample (Optional[int], optional): The number of rows to sample for validation. Defaults to 100.
        sampling_strategy (Optional[SamplingStrategy], optional): The strategy to use for sampling. Defaults to SamplingStrategy.RANDOM_SAMPLE.
        job_context (SnowparkJobContext, optional): The job context for the Snowpark job. Defaults to None.
        check_name (Optional[str], optional): The name of the checkpoint to retrieve the schema. Defaults to None.

    Returns:
        function: The decorated function.
    """

    def check_input_with_decorator(snowpark_fn):
        """
        Decorator to validate the schema of the input of a Snowpark function.

        Args:
            snowpark_fn (function): The Snowpark function to validate.

        Returns:
            function: The decorated function.
        """
        checkpoint_name = check_name
        if check_name is None:
            checkpoint_name = snowpark_fn.__name__

        def wrapper(*args, **kwargs):
            """
            Wrapper function to validate the schema of the input of a Snowpark function.

            Args:
                *args: The arguments to the Snowpark function.
                **kwargs: The keyword arguments to the Snowpark function.

            Returns:
                Any: The result of the Snowpark function.
            """
            # Run the sampled data in snowpark
            sampler = SamplingAdapter(
                job_context, sample_frac, sample_n, sampling_strategy
            )
            sampler.process_args(args)
            pandas_sample_args = sampler.get_sampled_pandas_args()
            telemetry = TelemetryManager()
            # Raises SchemaError on validation issues
            for arg in pandas_sample_args:
                if isinstance(arg, pandas.DataFrame):
                    try:
                        validator = DataFrameValidator()
                        is_valid, result = validator.validate(
                            pandera_schema, arg, validity_flag=True
                        )
                        if job_context is not None:
                            telemetry_data = {
                                "validation": is_valid,
                                "schema": [str(type) for type in result.dtypes.values],
                            }
                            telemetry.log_info(
                                "DataFrameSchemaValidator", telemetry_data
                            )
                            job_context.mark_pass(checkpoint_name)
                    except Exception as pandera_ex:
                        telemetry_data = {
                            "error": "Snowpark output schema validation error",
                            "pandera_ex": pandera_ex.message,
                        }
                        telemetry.log_error("DataFrameValidator_Error", telemetry_data)
                        raise SchemaValidationError(
                            "Snowpark schema input validation error",
                            job_context,
                            checkpoint_name,
                            pandera_ex,
                        )
            return snowpark_fn(*args, **kwargs)

        return wrapper

    return check_input_with_decorator

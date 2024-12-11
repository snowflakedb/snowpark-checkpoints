#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Callable, Optional, TypeVar

from pyspark.sql import DataFrame as SparkDataFrame

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.errors import SparkMigrationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)


fn = TypeVar("F", bound=Callable)


def check_with_spark(
    job_context: SnowparkJobContext,
    spark_function: fn,
    check_name: Optional[str] = None,
    sample_n: Optional[int] = 100,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    check_dtypes: Optional[bool] = True,
    check_with_precision: Optional[float] = True,
) -> Callable[[fn], fn]:
    """Validate function output with Spark instance.

    Will take the input snowpark dataframe of this function, sample data, convert
    it to a Spark dataframe and then execute `spark_function`. Subsequently
    the output of that function will be compared to the output of this function
    for the same sample of data.

    Args:
        job_context (SnowparkJobContext): The job context containing configuration and details for the validation.
        spark_function (fn): The equivalent PySpark function to compare against the Snowpark implementation.
        check_name (Optional[str], optional): A name for the checkpoint. Defaults to None.
        sample_n (Optional[int], optional): The number of rows for validation. Defaults to 100.
        sampling_strategy (Optional[SamplingStrategy], optional): The strategy used for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.
        check_dtypes (Optional[bool], optional): Enable data type consistency checks between Snowpark and PySpark.
            Defaults to True.
        check_with_precision (Optional[float], optional): Precision value to control numerical comparison precision.
            Defaults to True.

    Returns:
        Callable[[fn], fn]: A decorator that wraps the original Snowpark function with validation logic.

    """

    def check_with_spark_decorator(snowpark_fn):
        checkpoint_name = check_name
        if check_name is None:
            checkpoint_name = snowpark_fn.__name__

        def wrapper(*args, **kwargs):
            sampler = SamplingAdapter(
                job_context, sample_n=sample_n, sampling_strategy=sampling_strategy
            )
            sampler.process_args(args)
            snowpark_sample_args = sampler.get_sampled_snowpark_args()
            pyspark_sample_args = sampler.get_sampled_spark_args()
            # Run the sampled data in snowpark
            snowpark_test_results = snowpark_fn(*snowpark_sample_args, **kwargs)
            spark_test_results = spark_function(*pyspark_sample_args, **kwargs)
            _assert_return(
                snowpark_test_results, spark_test_results, job_context, checkpoint_name
            )
            # Run the original function in snowpark
            return snowpark_fn(*args, **kwargs)

        return wrapper

    return check_with_spark_decorator


def _assert_return(snowpark_results, spark_results, job_context, checkpoint_name):
    """Assert and validate the results from Snowpark and Spark transformations.

    Args:
        snowpark_results (Any): Results from the Snowpark transformation.
        spark_results (Any): Results from the Spark transformation to compare against.
        job_context (Any): Additional context about the job. Defaults to None.
        checkpoint_name (Any): Name of the checkpoint for logging. Defaults to None.

    Raises:
        AssertionError: If the Snowpark and Spark results do not match.
        TypeError: If the results cannot be compared.

    """
    if isinstance(snowpark_results, SnowparkDataFrame) and isinstance(
        spark_results, SparkDataFrame
    ):
        snowpark_df = snowpark_results.to_pandas()
        snowpark_df.columns = snowpark_df.columns.str.upper()
        spark_df = spark_results.toPandas()
        spark_df.columns = spark_df.columns.str.upper()
        if spark_df.shape != snowpark_df.shape:
            cmp = spark_df.merge(snowpark_df, indicator=True, how="left").loc[
                lambda x: x["_merge"] != "both"
            ]
            cmp = cmp.replace(
                {"left_only": "spark_only", "right_only": "snowpark_only"}
            )
        else:
            cmp = spark_df.compare(snowpark_df, result_names=("Spark", "snowpark"))

        if not cmp.empty:
            raise SparkMigrationError(
                "DataFrame difference:\n", job_context, checkpoint_name, cmp
            )
        job_context.mark_pass(checkpoint_name)
    else:
        if snowpark_results != spark_results:
            raise SparkMigrationError(
                "Return value difference:\n",
                job_context,
                checkpoint_name,
                f"{snowpark_results} != {spark_results}",
            )
        job_context.mark_pass(checkpoint_name)

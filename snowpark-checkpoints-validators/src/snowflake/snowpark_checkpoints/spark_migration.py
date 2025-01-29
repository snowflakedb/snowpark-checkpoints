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
from typing import Callable, Optional, TypeVar

import pandas as pd

from pyspark.sql import DataFrame as SparkDataFrame

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark.types import PandasDataFrame
from snowflake.snowpark_checkpoints.errors import SparkMigrationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)
from snowflake.snowpark_checkpoints.utils.constants import FAIL_STATUS, PASS_STATUS
from snowflake.snowpark_checkpoints.utils.telemetry import STATUS_KEY, report_telemetry
from snowflake.snowpark_checkpoints.utils.utils_checks import (
    _replace_special_characters,
    _update_validation_result,
)


fn = TypeVar("F", bound=Callable)


def check_with_spark(
    job_context: Optional[SnowparkJobContext],
    spark_function: fn,
    checkpoint_name: str,
    sample_number: Optional[int] = 100,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    output_path: Optional[str] = None,
) -> Callable[[fn], fn]:
    """Validate function output with Spark instance.

    Will take the input snowpark dataframe of this function, sample data, convert
    it to a Spark dataframe and then execute `spark_function`. Subsequently
    the output of that function will be compared to the output of this function
    for the same sample of data.

    Args:
        job_context (SnowparkJobContext): The job context containing configuration and details for the validation.
        spark_function (fn): The equivalent PySpark function to compare against the Snowpark implementation.
        checkpoint_name (str): A name for the checkpoint. Defaults to None.
        sample_number (Optional[int], optional): The number of rows for validation. Defaults to 100.
        sampling_strategy (Optional[SamplingStrategy], optional): The strategy used for sampling data.
            Defaults to SamplingStrategy.RANDOM_SAMPLE.
        output_path (Optional[str], optional): The path to store the validation results. Defaults to None.

    Returns:
        Callable[[fn], fn]: A decorator that wraps the original Snowpark function with validation logic.

    """

    def check_with_spark_decorator(snowpark_fn):
        _checkpoint_name = checkpoint_name
        if checkpoint_name is None:
            _checkpoint_name = snowpark_fn.__name__
        _checkpoint_name = _replace_special_characters(_checkpoint_name)

        def wrapper(*args, **kwargs):
            sampler = SamplingAdapter(
                job_context,
                sample_number=sample_number,
                sampling_strategy=sampling_strategy,
            )
            sampler.process_args(args)
            snowpark_sample_args = sampler.get_sampled_snowpark_args()
            pyspark_sample_args = sampler.get_sampled_spark_args()
            # Run the sampled data in snowpark
            snowpark_test_results = snowpark_fn(*snowpark_sample_args, **kwargs)
            spark_test_results = spark_function(*pyspark_sample_args, **kwargs)
            result, exception = _assert_return(
                snowpark_test_results,
                spark_test_results,
                job_context,
                _checkpoint_name,
                output_path,
            )
            if not result:
                raise exception from None
            # Run the original function in snowpark
            return snowpark_fn(*args, **kwargs)

        return wrapper

    return check_with_spark_decorator


@report_telemetry(
    params_list=["snowpark_results", "spark_results"],
    return_indexes=[(STATUS_KEY, 0)],
    multiple_return=True,
)
def _assert_return(
    snowpark_results, spark_results, job_context, checkpoint_name, output_path=None
) -> tuple[bool, Optional[Exception]]:
    """Assert and validate the results from Snowpark and Spark transformations.

    Args:
        snowpark_results (Any): Results from the Snowpark transformation.
        spark_results (Any): Results from the Spark transformation to compare against.
        job_context (Any): Additional context about the job. Defaults to None.
        checkpoint_name (Any): Name of the checkpoint for logging. Defaults to None.
        output_path (Optional[str], optional): The path to store the validation results. Defaults to None.

    Raises:
        AssertionError: If the Snowpark and Spark results do not match.
        TypeError: If the results cannot be compared.

    """
    if isinstance(snowpark_results, SnowparkDataFrame) and isinstance(
        spark_results, SparkDataFrame
    ):
        cmp = compare_spark_snowpark_dfs(spark_results, snowpark_results)

        if not cmp.empty:
            exception_result = SparkMigrationError(
                "DataFrame difference:\n", job_context, checkpoint_name, cmp
            )
            return False, exception_result
        job_context._mark_pass(checkpoint_name)
        _update_validation_result(checkpoint_name, PASS_STATUS, output_path)
        return True, None
    else:

        if snowpark_results != spark_results:
            exception_result = SparkMigrationError(
                "Return value difference:\n",
                job_context,
                checkpoint_name,
                f"{snowpark_results} != {spark_results}",
            )
            _update_validation_result(checkpoint_name, FAIL_STATUS, output_path)
            return False, exception_result
        job_context._mark_pass(checkpoint_name)
        _update_validation_result(checkpoint_name, PASS_STATUS, output_path)
        return True, None


def compare_spark_snowpark_dfs(
    spark_df: SparkDataFrame, snowpark_df: SnowparkDataFrame
) -> PandasDataFrame:
    """Compare two dataframes for equality.

    Args:
        spark_df (SparkDataFrame): The Spark dataframe to compare.
        snowpark_df (SnowparkDataFrame): The Snowpark dataframe to compare.

    Returns:Pandas DataFrame containing the differences between the two dataframes.

    """
    snowpark_df = snowpark_df.to_pandas()
    snowpark_df.columns = snowpark_df.columns.str.upper()
    spark_df = spark_df.toPandas()
    spark_df.columns = spark_df.columns.str.upper()
    spark_cols = set(spark_df.columns)
    snowpark_cols = set(snowpark_df.columns)
    cmp = pd.DataFrame([])
    left = spark_cols - snowpark_cols
    right = snowpark_cols - spark_cols
    if left != set():
        cmp = _compare_dfs(spark_df, snowpark_df, "spark", "snowpark")
    if right != set():
        right_cmp = _compare_dfs(snowpark_df, spark_df, "snowpark", "spark")
        cmp = right_cmp if cmp.empty else pd.concat([cmp, right_cmp], ignore_index=True)
    if left == set() and right == set():
        if spark_df.shape == snowpark_df.shape:
            cmp = spark_df.compare(snowpark_df, result_names=("spark", "snowpark"))
        else:
            cmp = spark_df.merge(snowpark_df, indicator=True, how="outer").loc[
                lambda x: x["_merge"] != "both"
            ]
            cmp = cmp.replace(
                {"left_only": "spark_only", "right_only": "snowpark_only"}
            )

    return cmp


def _compare_dfs(
    df_a: pd.DataFrame, df_b: pd.DataFrame, left_label: str, right_label: str
) -> PandasDataFrame:
    """Compare two dataframes for equality.

    Args:
        df_a (PandasDataFrame): The first dataframe to compare.
        df_b (PandasDataFrame): The second dataframe to compare.
        left_label (str): The label for the first dataframe.
        right_label (str): The label for the second dataframe.

    :return: Pandas DataFrame containing the differences between the two dataframes.

    """
    df_a["side"] = "a"
    df_b["side"] = "b"
    a_only = [col for col in df_a.columns if col not in df_b.columns] + ["side"]
    b_only = [col for col in df_b.columns if col not in df_a.columns] + ["side"]
    cmp = (
        df_a[a_only]
        .merge(df_b[b_only], indicator=True, how="left")
        .loc[lambda x: x["_merge"] != "both"]
    )
    cmp = cmp.replace(
        {"left_only": f"{left_label}_only", "right_only": f"{right_label}_only"}
    )
    cmp = cmp.drop(columns="side")
    return cmp

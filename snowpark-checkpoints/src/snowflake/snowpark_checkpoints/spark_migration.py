#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import ast
import atexit
import inspect
import re
import shutil
import types

from typing import Callable, Optional, TypeVar

from pyspark.sql import DataFrame as SparkDataFrame
from tqdm import tqdm

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.errors import SparkMigrationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import (
    SamplingAdapter,
    SamplingStrategy,
)


fn = TypeVar("F", bound=Callable)

patches = []
patch_cache = {}


def _string_to_function(source):
    tree = ast.parse(source)
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef):
        raise ValueError("provided code fragment is not a single function")
    co = compile(tree, "custom.py", "exec")
    # first constant should be the code object for the function
    return types.FunctionType(co.co_consts[0], {})


def _translate_spark_to_snowpark(job_context, spark_source: str):
    job_context.autopbar.set_description("Calling cortex functions")
    llm_query = job_context.snowpark_session.sql(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', "
        + "'Convert the following spark function code to snowpark,"
        + "keeping the function name and signature the same as the "
        + f"original```{spark_source}```');"
    )
    llm_answer = llm_query.to_pandas()
    llm_answer.columns = ["ans"]
    llm_answer = llm_answer["ans"].iloc[0]
    llm_answer = llm_answer.split("```")[1].replace("python\n", "")
    job_context.autopbar.update(10)
    return llm_answer


def _spark_fn_to_snowpark_fn(job_context, spark_fn: callable):
    spark_fn_source = inspect.getsource(spark_fn)
    spark_fn_source = re.sub(r"@auto_migrate[^)]*\)\n", "", spark_fn_source)
    original_fn_name = spark_fn.__name__
    snowpark_fn_source = _translate_spark_to_snowpark(job_context, spark_fn_source)
    snowpark_fn_source = re.sub(
        r"def [^(]*\(", f"def {original_fn_name}(", snowpark_fn_source
    )
    job_context.autopbar.update(10)
    job_context.autopbar.set_description("Initial translation complete")
    return (_string_to_function(snowpark_fn_source), snowpark_fn_source)


def _update_snowpark_code():
    if len(patches) <= 0:
        return
    patch_queue = {}
    for patch in patches:
        (spark_fn, snowpark_fn_source) = patch
        source_lines = inspect.getsourcelines(spark_fn)
        line_start = source_lines[1]
        line_count = len(source_lines[0])
        line_end = line_start + line_count

        patch_queue[line_start] = {
            "start": line_start,
            "end": line_end,
            "source": snowpark_fn_source,
        }

    filename = inspect.getfile(spark_fn)
    out_filename = f"{filename}.migrated"
    save_filename = f"{filename}.original"
    shutil.copyfile(filename, save_filename)
    source_lines = inspect.getsourcelines(spark_fn)

    with open(filename) as file:
        with open(out_filename, "w") as out_file:
            line_num = 1
            line_end = 0
            line_start = 0
            for line in file:
                if line_num in patch_queue:
                    out_file.write("######################################\n")
                    out_file.write("## Auto-migrated Snowpark code: \n")
                    out_file.write(patch_queue[line_num]["source"])
                    out_file.write("######################################\n")
                    out_file.write("## Original Spark code: \n")
                    line_end = patch_queue[line_num]["end"]
                    line_start = patch_queue[line_num]["start"]
                elif line_num > line_start and line_num < line_end:
                    out_file.write("## ")
                    out_file.write(line)
                    pass
                else:
                    line_end = 0
                    line_start = 0
                    out_file.write(line)
                line_num = line_num + 1
    shutil.copyfile(out_filename, filename)
    return None


atexit.register(_update_snowpark_code)


def _patch_cache_key(spark_fn):
    source_lines = inspect.getsourcelines(spark_fn)
    filename = inspect.getfile(spark_fn)
    function_start = source_lines[1]
    return f"{filename}:{function_start}"


def auto_migrate(
    job_context: SnowparkJobContext,
) -> Callable[[fn], fn]:
    """Auto migrate a function written for spark.

    Will take the wrapped function and
    generate a snowpark equivalent.
    """

    def auto_migrate_decorator(spark_fn):
        patch_key = _patch_cache_key(spark_fn)
        if patch_key in patch_cache:
            return patch_cache[patch_key]
        job_context.autopbar = tqdm(total=100)
        job_context.autopbar.set_description("Translating spark -> snowpark")
        (snowpark_fn, snowpark_fn_source) = _spark_fn_to_snowpark_fn(
            job_context, spark_fn
        )
        patches.append((spark_fn, snowpark_fn_source))
        patch_cache[patch_key] = snowpark_fn
        job_context.autopbar.update(30)

        def wrapper(*args, **kwargs):
            job_context.autopbar.set_description("Validating code with sampled data")
            sampler = SamplingAdapter(
                job_context,
                sample_n=100,
                sampling_strategy=SamplingStrategy.RANDOM_SAMPLE,
            )
            sampler.process_args(args)
            # snowpark_sample_args = sampler.get_sampled_snowpark_args()
            # pyspark_sample_args = sampler.get_sampled_spark_args()

            # Run the sampled data in snowpark
            # snowpark_test_results = snowpark_fn(*snowpark_sample_args, **kwargs)
            # snowpark_test_results = snowpark_sample_args[0]
            # spark_test_results = spark_fn(*pyspark_sample_args, **kwargs)

            # Run the original function in snowpark
            job_context.autopbar.update(50)
            job_context.autopbar.set_description("Spark Code Converted")
            job_context.autopbar.close()
            return snowpark_fn(*args, **kwargs)

        return wrapper

    return auto_migrate_decorator


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

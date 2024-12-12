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
    print("Generated Source Code:\n", source)
    tree = ast.parse(source)
    # Prune any leading imports
    while not isinstance(tree.body[0], ast.FunctionDef):
        tree.body.pop(0)
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef):
        raise ValueError("provided code fragment is not a single function")
    co = compile(tree, "custom.py", "exec", dont_inherit=False)
    # first constant should be the code object for the function
    return types.FunctionType(co.co_consts[0], {})


def _translate_spark_to_snowpark(job_context, spark_source: str):
    job_context.autopbar.set_description("Calling cortex functions")

    # escape single quotes
    spark_fn_source = re.sub(r"'", "''", spark_source)

    query_text = (
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', "
        + "'Convert the following spark function to a single snowpark function, "
        + "keeping the function name and signature the same as the "
        + "original but excluding any import statements inside or outside of the function.\n"
        + f"```{spark_fn_source}```');"
    )
    llm_query = job_context.snowpark_session.sql(query_text)
    llm_answer = None
    try:
        llm_answer = llm_query.to_pandas()
    except Exception as e:
        print("Unable to translate:\n", spark_fn_source)
        print("Query: ", query_text)
        print("Exception:\n ", e)
        raise e
    llm_answer.columns = ["ans"]
    llm_answer = llm_answer["ans"].iloc[0]
    llm_answer = llm_answer.split("```")[1].replace("python\n", "").replace("''", "'")
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


def _validate_snowpark_code(
    job_context, snowpark_fn, snowpark_sample_args, spark_results
):
    snowpark_test_results = None
    snowpark_test_error = None
    # Run the sampled data in snowpark
    try:
        job_context.autopbar.set_description("Collecting Snowpark Results")
        snowpark_test_results = snowpark_fn(*snowpark_sample_args).to_pandas()
    except Exception as e:
        snowpark_test_error = e
        pass

    if snowpark_test_results is None:
        # did not compile, retry
        return ("exec_error", snowpark_test_error)
    if spark_results is None or snowpark_test_results is None:
        return ("cannot_compare", 0)
    diffs = 0
    try:
        diffs = spark_results.compare(snowpark_test_results).size
    except Exception as _e:
        pass
    return ("compare", diffs)


def _try_runtime_fix(job_context, snowpark_fn_source, data):
    job_context.autopbar.set_description("Asking LLM to fix runtime errors")

    # escape single quotes
    snowpark_fn_source = re.sub(r"'", "''", snowpark_fn_source)
    runtime_error = re.sub(r"'", "''", str(data))

    query_text = (
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', "
        + "'The following function produced an error during execution. "
        + f'The error was "{runtime_error}". Please fix and return a new '
        + "version of the function, without adding any new import statements."
        + "Assume that ''F'' does not need to be defined, and it can be removed from the code. \n"
        + f"```{snowpark_fn_source}```');"
    )
    llm_query = job_context.snowpark_session.sql(query_text)
    llm_answer = None
    try:
        llm_answer = llm_query.to_pandas()
    except Exception as e:
        print("Unable to translate:\n", snowpark_fn_source)
        print("Query: ", query_text)
        print("Exception:\n ", e)
        raise e
    llm_answer.columns = ["ans"]
    llm_answer = llm_answer["ans"].iloc[0]
    llm_answer = llm_answer.split("```")[1].replace("python\n", "").replace("''", "'")
    job_context.autopbar.update(10)
    return (_string_to_function(llm_answer), llm_answer)


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

        job_context.autopbar.update(30)

        def wrapper(*args, **kwargs):
            job_context.autopbar.set_description("Validating code with sampled data")
            sampler = SamplingAdapter(
                job_context,
                sample_n=100,
                sampling_strategy=SamplingStrategy.RANDOM_SAMPLE,
            )
            sampler.process_args(args)
            snowpark_sample_args = sampler.get_sampled_snowpark_args()
            pyspark_sample_args = sampler.get_sampled_spark_args()
            spark_test_results = None
            try:
                job_context.autopbar.update(10)
                job_context.autopbar.set_description("Collecting Spark Results")
                spark_test_results = spark_fn(*pyspark_sample_args, **kwargs).toPandas()
            except Exception:
                pass
            snowpark_fn_curr = snowpark_fn
            snowpark_fn_source_curr = snowpark_fn_source
            for i in range(4):
                job_context.autopbar.set_description(f"Trying again {i}")
                (result, data) = _validate_snowpark_code(
                    job_context,
                    snowpark_fn_curr,
                    snowpark_sample_args,
                    spark_test_results,
                )
                if result == "exec_error":
                    try:
                        (snowpark_fn_curr, snowpark_fn_source_curr) = _try_runtime_fix(
                            job_context, snowpark_fn_source_curr, data
                        )
                    except Exception as _e:
                        pass
                if result == "cannot_compare":
                    # (snowpark_fn_curr, snowpark_fn_source_curr) =
                    # _try_compare_fix(job_context, snowpark_fn_source_curr, data)
                    break
                if result == "compare":
                    # (snowpark_fn_curr, snowpark_fn_source_curr) =
                    # _try_improve_fix(job_context, snowpark_fn_source_curr, data)
                    break

            # Run the original function in snowpark
            job_context.autopbar.update(50)
            job_context.autopbar.set_description("Spark Code Converted")
            patches.append((spark_fn, snowpark_fn_source_curr))
            patch_cache[patch_key] = snowpark_fn_curr
            job_context.autopbar.close()
            return snowpark_fn_curr(*args, **kwargs)

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

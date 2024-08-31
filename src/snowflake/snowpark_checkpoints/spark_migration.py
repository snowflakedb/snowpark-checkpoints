

from typing import Callable, Optional, TypeVar
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from pyspark.sql import DataFrame as SparkDataFrame
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from enum import Enum

from snowflake.snowpark_checkpoints.snowpark_sampler import SamplingAdapter, SamplingStrategy
fn = TypeVar("F", bound=Callable)

class SparkMigrationError(Exception):
    def __init__(self, 
                 message, 
                 job_name = None, 
                 checkpoint_name = None, 
                 data = None):
        super().__init__(f"Job: {job_name} Checkpoint: {checkpoint_name}\n{message}")
        self.data = data
        self.job_name = job_name
        self.checkpoint_name = checkpoint_name

def check_with_spark(
    job_context: SnowparkJobContext,
    spark_function: fn,
    check_name: Optional[str] = None,
    sample: Optional[int] = 100,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    check_dtypes: Optional[bool] = True,
    check_with_precision: Optional[float] = True,
) -> Callable[[fn], fn]:
    """Validate function output with spark instance.

    Will take the input snowpark dataframe of this function, sample data, convert
    it to a spark dataframe and then execute `spark_function`. Subsequently
    the output of that function will be compared to the output of this function
    for the same sample of data.

    :param sample: validate a random sample of n rows. Rows overlapping
        with `head` or `tail` are de-duplicated.
    :returns: wrapped function
    """
    def check_with_spark_decorator(snowpark_fn):
        checkpoint_name = check_name
        if check_name == None:
            checkpoint_name = snowpark_fn.__name__
        def wrapper(*args,**kwargs):
            sampler = SamplingAdapter(job_context, sample, sampling_strategy)
            sampler.process_args(args)
            snowpark_sample_args = sampler.get_sampled_snowpark_args()
            pyspark_sample_args = sampler.get_sampled_spark_args()
            # Run the sampled data in snowpark
            snowpark_test_results = snowpark_fn(*snowpark_sample_args, **kwargs)
            spark_test_results = spark_function(*pyspark_sample_args, **kwargs)
            assert_return(snowpark_test_results, spark_test_results, job_context.job_name, checkpoint_name)
            # Run the original function in snowpark
            return snowpark_fn(*args, **kwargs)
        return wrapper
    return check_with_spark_decorator


def assert_return(snowpark_results, spark_results, job_name, checkpoint_name):
    if ( isinstance(snowpark_results, SnowparkDataFrame) and
         isinstance(spark_results, SparkDataFrame)
        ):
        snowpark_df = snowpark_results.to_pandas()
        spark_df = spark_results.toPandas()
        cmp = spark_df.compare(snowpark_df, result_names=("left", "right"))
        if not cmp.empty:
            raise SparkMigrationError(f"DataFrame difference:\n{cmp.to_string()}", job_name, checkpoint_name, cmp)
    else:
        if snowpark_results != spark_results:
            raise SparkMigrationError(f"Return value difference:\n{snowpark_results} != {spark_results}", job_name, checkpoint_name, f"{snowpark_results} != {spark_results}")
        
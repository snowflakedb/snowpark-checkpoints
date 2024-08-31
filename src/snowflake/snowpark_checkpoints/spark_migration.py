

from typing import Callable, Optional, TypeVar
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from pyspark.sql import DataFrame as SparkDataFrame
from snowflake.snowpark import DataFrame as SnowparkDataFrame
import pandas
fn = TypeVar("F", bound=Callable)

class SparkMigrationError(Exception):
    def __init__(self, message, job_name, checkpoint_name, data):
        super().__init__(message)
        self.data = data
        self.job_name = job_name
        self.checkpoint_name = checkpoint_name

def check_with_spark(
    job_context: SnowparkJobContext,
    spark_function: fn,
    check_name: Optional[str] = None,
    sample: Optional[int] = 100,
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
            input_args = args
            snowpark_sample_args = []
            pyspark_sample_args = []
            # create the intermediate pandas
            # data frame for the test data
            for arg in input_args:
                if isinstance(arg, SnowparkDataFrame):
                    df_sample = arg.sample(n=sample).to_pandas()
                    snowpark_df = job_context.snowpark_session.create_dataframe(df_sample)
                    snowpark_sample_args.append(snowpark_df)
                    pyspark_df = job_context.spark_session.createDataFrame(df_sample)
                    pyspark_sample_args.append(pyspark_df)
                else:
                    snowpark_sample_args.append(arg)

            # Run the sampled data in snowpark
            snowpark_test_results = snowpark_fn(*snowpark_sample_args, **kwargs)
            spark_test_results = spark_function(*pyspark_sample_args, **kwargs)
            assert_return(snowpark_test_results, spark_test_results, job_context, checkpoint_name)
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
            raise SparkMigrationError(f"DataFrame difference {cmp.to_string()}", job_name, checkpoint_name, cmp)
    else:
        if snowpark_results != spark_results:
            raise SparkMigrationError(f"Return value difference {snowpark_results} != {spark_results}", job_name, checkpoint_name, f"{snowpark_results} != {spark_results}")
        
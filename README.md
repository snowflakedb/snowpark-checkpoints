# snowpark-checkpoints
Snowpark Python / Spark Migration Testing Tools

## check_with_spark decorator
The `check_with_spark` decorator will convert any Snowpark DataFrame
arguments to a function, sample, and convert them to PySpark DataFrames

The check will then execute a provided spark function which mirrors the
functionality of the new snowpark function and compare the outputs
between the two implementations.

Assuming the spark function and snowpark functions are semantically
identical this allows for verification of those functions on real,
sampled data.

### Usage
```
session = Session.builder.getOrCreate()
job_context = SnowparkJobContext(session)

def mirrored_spark_fn(df:SnowparkDataFrame):
    ...

@check_with_spark(job_context, spark_function=mirrored_spark_fn)
def snowpark_fn(df:SnowparkDataFrame):
    ...
```
### Arguments
 * `job_context`: SnowparkJobContext - contains 
 * `spark_function`: fn - spark function which mirrors this function
 * `check_name`: Optional[str] = None - checkpoint name, defaults to function name
 * `sample`: Optional[int] = 100 - Number of rows to sample from each Snowpark DF
 * `check_dtypes`: Not Implemented
 * `check_with_precision`: Not Implemented

## References
* #spark-lift-and-shift
* #snowpark-migration-discussion
* One-Pager [Checkpoints for Spark / Snowpark](https://docs.google.com/document/d/1obeiwm2qjIA2CCCjP_2U4gaZ6wXe0NkJoLIyMFAhnOM/edit)
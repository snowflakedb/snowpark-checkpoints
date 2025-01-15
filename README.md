# snowpark-checkpoints

Snowpark Python / Spark Migration Testing Tools

---
**NOTE**

This package is on Private Preview.

---

# Data Collection from Spark Pipelines

The `snowpark-checkpoints-collector` package can collect
schema and check information from a spark pipeline and
record those results into a set of JSON files corresponding to different intermediate dataframes. These files can be inspected manually
and handed over to teams implementing the snowpark pipeline. The `snowpark-checkpoints-collector` package is designed to have minimal
dependencies and the generated files are meant to be inspected by security
teams.

On the snowpark side the `snowpark-checkpoints` package can use these files to perform schema and data validation checks against snowpark dataframes at the same, intermediate logical "checkpoints".

## collect_dataframe_schema

```
from snowflake.snowpark_checkpoints_collector import collect_dataframe_schema;
collect_dataframe_schema(df:SparkDataFrame,
                              checkpoint_name,
                              sample=0.1)
```

- df - the spark data frame to collect the schema from
- checkpoint_name - the name of the "checkpoint". Generated JSON files
  will have the name "snowpark-[checkpoint_name]-schema.json"
- sample - sample size of the spark data frame to use to generate the schema

### Validate DataFrame Schema from File

The `validate_dataframe_checkpoint` function validates a Snowpark DataFrame against a checkpoint schema file or dataframe.

```python
from snowflake.snowpark_checkpoints import check_dataframe_schema_file

validate_dataframe_checkpoint(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    job_context: Optional[SnowparkJobContext] = None,
    mode: Optional[CheckpointMode] = CheckpointMode.SCHEMA,
    custom_checks: Optional[dict[Any, Any]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
)
```

- `df`: Snowpark DataFrame to validate.
- `checkpoint_name`: Name of the checkpoint schema file or DataFrame.
- `job_context`: Snowpark job context.
- `mode`: Checkpoint mode (schema or data).
- `custom_checks`: Custom checks to perform.
- `skip_checks`: Checks to skip.
- `sample_frac`: Fraction of the DataFrame to sample.
- `sample_number`: Number of rows to sample.
- `sampling_strategy`: Sampling strategy to use.

### Check with Spark Decorator

The `check_with_spark` decorator converts any Snowpark DataFrame arguments to a function, samples them, and converts them to PySpark DataFrames. It then executes a provided Spark function and compares the outputs between the two implementations.

```python
from snowflake.snowpark_checkpoints import check_with_spark

@check_with_spark(
    job_context: Optional[SnowparkJobContext],
    spark_function: Callable,
    checkpoint_name: str,
    sample_number: Optional[int] = 100,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    check_dtypes: Optional[bool] = False,
    check_with_precision: Optional[bool] = False
)
def snowpark_fn(df: SnowparkDataFrame):
    ...
```

- `job_context`: Snowpark job context.
- `spark_function`: PySpark function to execute.
- `checkpoint_name`: Name of the check.
- `sample_number`: Number of rows to sample.
- `sampling_strategy`: Sampling strategy to use.
- `check_dtypes`: Check data types.
- `check_with_precision`: Check with precision.

## Pandera Snowpark Decorators

The decorators `@check_input_with` and `@check_output_with` allow
for sampled schema validation of snowpark dataframes in the input arguments or
in the return value.

```python
from snowflake.snowpark_checkpoints import check_input_with, check_output_with

@check_input_with(
    pandera_schema: DataFrameSchema,
    checkpoint_name: Optional[str] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = 100,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
)
def snowpark_fn(df: SnowparkDataFrame):
    ...

@check_output_with(
    pandera_schema: DataFrameSchema,
    checkpoint_name: Optional[str] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
)
def snowpark_fn(df: SnowparkDataFrame):
    ...
```

## Run Demos

### Requirements

- Python >= 3.9
- OpenJDK 21.0.2
- Snow CLI: The default connection needs to have a database and a schema. After running the app, a table called SNOWPARK_CHECKPOINTS_REPORT will be created.

### Steps

1. Create a Python environment with Python 3.9 or higher in the Demos dir.
2. Build the Python snowpark-checkpoints and snowpark-checkpoints-collector packages. Learn more.

```cmd
cd package_dir
pip install -e .
python3 -m pip install --upgrade build
python3 -m build
```

3. In Demos dir, run:
   pip install "snowflake-connector-python[pandas]"
4. First, run the PySpark demo:
   python demo_pyspark_pipeline.py
   This will generate the JSON schema files. Then, run the Snowpark demo:
   python demo_snowpark_pipeline.py

## References

- #spark-lift-and-shift
- #snowpark-migration-discussion
- One-Pager [Checkpoints for Spark / Snowpark](https://docs.google.com/document/d/1obeiwm2qjIA2CCCjP_2U4gaZ6wXe0NkJoLIyMFAhnOM/edit)

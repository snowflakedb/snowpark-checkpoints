# snowpark-checkpoints

Snowpark Python / Spark Migration Testing Tools

## TODO

- create a spark-pipeline side pandera schema collector
- clearly distinguish between schema-validation mode and parallel execution mode
- add the streamlit app

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

## check_dataframe_schema_file

The `check_dataframe_schema_file` function can be used to validate a Snowpark DataFrame against a checkpoint schema file.

```python
check_dataframe_schema_file(df: SnowparkDataFrame,
                            checkpoint_name: str,
                            custom_checks: Optional[dict[Any, Any]] = None,
                            skip_checks: Optional[dict[Any, Any]] = None,
                            job_context: Optional[SnowparkJobContext] = None,
                            sample_frac: Optional[float] = 0.1,
                            sample_number: Optional[int] = None,
                            sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE)
```

- df - The DataFrame to be validated.
- checkpoint_name - The name of the checkpoint to retrieve the schema.
- skip_checks - Checks to be skipped.
- job_context - Context for job-related operations.
- sample_frac - Fraction of data to sample.
- sample_number - Number of rows to sample.
- sampling_strategy - Strategy for sampling data.

## check_with_spark Decorator

The `check_with_spark` decorator will convert any Snowpark DataFrame
arguments to a function, sample, and convert them to PySpark DataFrames

The check will then execute a provided spark function which mirrors the
functionality of the new snowpark function and compare the outputs
between the two implementations.

Assuming the spark function and snowpark functions are semantically
identical this allows for verification of those functions on real,
sampled data.

```python
check_with_spark(job_context: Optional[SnowparkJobContext],
                 spark_function: Callable,
                 check_name: Optional[str] = None,
                 sample_number: Optional[int] = 100,
                 sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
                 check_dtypes: Optional[bool] = False,
                 check_with_precision: Optional[bool] = False)

```

- job_context - The context for job-related operations.
- spark_function - The function to be executed with PySpark.
- check_name - The name of the checkpoint.
- sample_number - Number of rows to sample from each Snowpark DataFrame.
- sampling_strategy - Strategy for sampling data.
- check_dtypes - Check data types.
- check_with_precision - Check with precision.

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

## Pandera Snowpark Decorators

The decorators `@check_input_with` and `@check_output_with` allow
for sampled schema validation of snowpark dataframes in the input arguments or
in the return value.

### Example

The following will result in a pandera SchemaError:
`pandera.errors.SchemaError: expected series 'COLUMN1' to have type int8, got object`

```
    df = pd.DataFrame({
        "COLUMN1": [1, 4, 0, 10, 9],
        "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
    })

    out_schema = DataFrameSchema({
        "COLUMN1": Column(int8,
                        Check(lambda x: 0 <= x <= 10, element_wise=True)),
        "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
    })

    @check_output_with(out_schema)
    def preprocessor(dataframe:SnowparkDataFrame):
        return dataframe.with_column("COLUMN1", lit('Some bad data yo'))

    session = Session.builder.getOrCreate()
    sp_dataframe = session.create_dataframe(df)

    preprocessed_dataframe = preprocessor(sp_dataframe)
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

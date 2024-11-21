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

## collect_df_schema

```
from snowflake.snowpark_checkpoints_collector import collect_df_schema;
collect_df_schema(df:SparkDataFrame,
                              checkpoint_name,
                              sample=0.1)
```

- df - the spark data frame to collect the schema from
- checkpoint_name - the name of the "checkpoint". Generated JSON files
  will have the name "snowpark-[checkpoint_name]-schema.json"
- sample - sample size of the spark data frame to use to generate the schema

## check_df_schema_file

```
check_df_schema_file(df:SnowparkDataFrame,
                            checkpoint_name:str,
                            job_context:SnowparkJobContext = None,
                            sample: Optional[int] = 100,
                            sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE)
```

- df - snowpark data frame to compare against the file schema
- checkpoint_name - the name of the "checkpoint". Generated JSON files
  will have the name "snowpark-[checkpoint_name]-schema.json"
- job_context - used to record migration results in snowflake, if desired
- sample, sampling_strategy - strategy used to sample the snowpark data frame

## check_with_spark Decorator

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

- `job_context`: SnowparkJobContext - contains
- `spark_function`: fn - spark function which mirrors this function
- `check_name`: Optional[str] = None - checkpoint name, defaults to function name
- `sample`: Optional[int] = 100 - Number of rows to sample from each Snowpark DF
- `sampling_strategy`: either SamplingStrategy.RANDOM_SAMPLE or SamplingStrategy.LIMIT, defaults to RANDOM_SAMPLE, but LIMIT may be faster
- `check_dtypes`: Not Implemented
- `check_with_precision`: Not Implemented

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
    sp_df = session.create_dataframe(df)

    preprocessed_df = preprocessor(sp_df)
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
python3 -m pip install --upgrade buildpython3 -m build
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

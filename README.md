# snowpark-checkpoints
Snowpark Python / Spark Migration Testing Tools

## TODO
- create a spark-pipeline side pandera schema collector
- create a snowpark-pipeline side for using generated schemas from spark side
- clearly distinguish between schema-validation mode and parallel execution mode
- add the streamlit app 

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
 * `job_context`: SnowparkJobContext - contains 
 * `spark_function`: fn - spark function which mirrors this function
 * `check_name`: Optional[str] = None - checkpoint name, defaults to function name
 * `sample`: Optional[int] = 100 - Number of rows to sample from each Snowpark DF
 * `sampling_strategy`: either SamplingStrategy.RANDOM_SAMPLE or SamplingStrategy.LIMIT, defaults to RANDOM_SAMPLE, but LIMIT may be faster
 * `check_dtypes`: Not Implemented
 * `check_with_precision`: Not Implemented


## Pandera Snowpark Decorators
The decorators `@check_input_with_pandera` and `@check_output_with_pandera` allow
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

    @check_output_with_pandera(out_schema)
    def preprocessor(dataframe:SnowparkDataFrame):
        return dataframe.with_column("COLUMN1", lit('Some bad data yo'))
    
    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    preprocessed_df = preprocessor(sp_df)
```


## References
* #spark-lift-and-shift
* #snowpark-migration-discussion
* One-Pager [Checkpoints for Spark / Snowpark](https://docs.google.com/document/d/1obeiwm2qjIA2CCCjP_2U4gaZ6wXe0NkJoLIyMFAhnOM/edit)
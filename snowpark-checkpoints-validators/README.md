# Snowpark Checkpoints Validators

---
**NOTE**

This package is on Private Preview.

---

**snowpark-checkpoints-validators** is a package designed to validate Snowpark DataFrames against predefined schemas and checkpoints. This package ensures data integrity and consistency by performing schema and data validation checks at various stages of a Snowpark pipeline.

## Features

- Validate Snowpark DataFrames against predefined Pandera schemas.
- Perform custom checks and skip specific checks as needed.
- Generate validation results and log them for further analysis.
- Support for sampling strategies to validate large datasets efficiently.
- Integration with PySpark for cross-validation between Snowpark and PySpark DataFrames.

## Functionalities

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

### Usage Example

```python
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints import validate_dataframe_checkpoint

session = Session.builder.getOrCreate()
df = session.read.format("csv").load("data.csv")

validate_dataframe_checkpoint(
    df,
    "schema_checkpoint",
    job_context=session,
    mode=CheckpointMode.SCHEMA,
    sample_frac=0.1,
    sampling_strategy=SamplingStrategy.RANDOM_SAMPLE
)
```

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

### Usage Example

```python
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints import check_with_spark

session = Session.builder.getOrCreate()
df = session.read.format("csv").load("data.csv")

@check_with_spark(
    job_context=session,
    spark_function=lambda df: df.withColumn("COLUMN1", df["COLUMN1"] + 1),
    checkpoint_name="Check_Column1_Increment",
    sample_number=100,
    sampling_strategy=SamplingStrategy.RANDOM_SAMPLE,
)
def increment_column1(df: SnowparkDataFrame):
    return df.with_column("COLUMN1", df["COLUMN1"] + 1)

increment_column1(df)
```

### Pandera Snowpark Decorators

The decorators `@check_input_with` and `@check_output_with` allow for sampled schema validation of Snowpark DataFrames in the input arguments or in the return value.

```python
from snowflake.snowpark_checkpoints import check_input_with, check_output_with

@check_input_with(
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext],
)
def snowpark_fn(df: SnowparkDataFrame):
    ...

@check_output_with(
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext],
)
def snowpark_fn(df: SnowparkDataFrame):
    ...
```

- `pandera_schema`: Pandera schema to validate.
- `checkpoint_name`: Name of the checkpoint schema file or DataFrame.
- `sample_frac`: Fraction of the DataFrame to sample.
- `sample_number`: Number of rows to sample.
- `sampling_strategy`: Sampling strategy to use.
- `job_context`: Snowpark job context.

### Usage Example

The following will result in a Pandera `SchemaError`:

```python
from pandas import DataFrame as PandasDataFrame
from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints import check_output_with

df = PandasDataFrame({
    "COLUMN1": [1, 4, 0, 10, 9],
    "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
})

out_schema = DataFrameSchema({
    "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
    "COLUMN2": Column(float, Check(lambda x: x < -1.2)),
})

@check_output_with(out_schema, "output_schema_checkpoint")
def preprocessor(dataframe: SnowparkDataFrame):
    return dataframe.with_column("COLUMN1", lit('Some bad data yo'))

session = Session.builder.getOrCreate()
sp_dataframe = session.create_dataframe(df)

preprocessed_dataframe = preprocessor(sp_dataframe)
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

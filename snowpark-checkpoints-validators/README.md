# snowpark-checkpoints-validators

---
##### This package is on Public Preview.
---

**snowpark-checkpoints-validators** is a package designed to validate Snowpark DataFrames against predefined schemas and checkpoints. This package ensures data integrity and consistency by performing schema and data validation checks at various stages of a Snowpark pipeline.

---
## Install the library
```bash
pip install snowpark-checkpoints-validators
```
This package requires PySpark to be installed in the same environment. If you do not have it, you can install PySpark alongside Snowpark Checkpoints by running the following command:
```bash
pip install "snowpark-checkpoints-validators[pyspark]"
```
---

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
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.utils.constant import (
    CheckpointMode,
)
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from typing import Any, Optional

# Signature of the function
def validate_dataframe_checkpoint(
    df: SnowparkDataFrame,
    checkpoint_name: str,
    job_context: Optional[SnowparkJobContext] = None,
    mode: Optional[CheckpointMode] = CheckpointMode.SCHEMA,
    custom_checks: Optional[dict[Any, Any]] = None,
    skip_checks: Optional[dict[Any, Any]] = None,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    output_path: Optional[str] = None,
):
...
```

- `df`: Snowpark dataframe to validate.
- `checkpoint_name`: Name of the checkpoint schema file or dataframe.
- `job_context`: Snowpark job context.
- `mode`: Checkpoint mode (schema or data).
- `custom_checks`: Custom checks to perform.
- `skip_checks`: Checks to skip.
- `sample_frac`: Fraction of the dataframe to sample.
- `sample_number`: Number of rows to sample.
- `sampling_strategy`: Sampling strategy to use.
- `output_path`: Output path for the checkpoint report.

### Usage Example

```python
from snowflake.snowpark import Session
from snowflake.snowpark_checkpoints.utils.constant import (
    CheckpointMode,
)
from snowflake.snowpark_checkpoints.checkpoint import validate_dataframe_checkpoint
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from pyspark.sql import SparkSession

session = Session.builder.getOrCreate()
job_context = SnowparkJobContext(
    session, SparkSession.builder.getOrCreate(), "job_context", True
)
df = session.read.format("csv").load("data.csv")

validate_dataframe_checkpoint(
    df,
    "schema_checkpoint",
    job_context=job_context,
    mode=CheckpointMode.SCHEMA,
    sample_frac=0.1,
    sampling_strategy=SamplingStrategy.RANDOM_SAMPLE
)
```

### Check with Spark Decorator

The `check_with_spark` decorator converts any Snowpark dataframe arguments to a function, samples them, and converts them to PySpark dataframe. It then executes a provided Spark function and compares the outputs between the two implementations.

```python
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from typing import Callable, Optional, TypeVar

fn = TypeVar("F", bound=Callable)

# Signature of the decorator
def check_with_spark(
    job_context: Optional[SnowparkJobContext],
    spark_function: fn,
    checkpoint_name: str,
    sample_number: Optional[int] = 100,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    output_path: Optional[str] = None,
) -> Callable[[fn], fn]:
    ...
```

- `job_context`: Snowpark job context.
- `spark_function`: PySpark function to execute.
- `checkpoint_name`: Name of the check.
- `sample_number`: Number of rows to sample.
- `sampling_strategy`: Sampling strategy to use.
- `output_path`: Output path for the checkpoint report.

### Usage Example

```python
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.spark_migration import check_with_spark
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession

session = Session.builder.getOrCreate()
job_context = SnowparkJobContext(
    session, SparkSession.builder.getOrCreate(), "job_context", True
)

def my_spark_scalar_fn(df: SparkDataFrame):
    return df.count()

@check_with_spark(
    job_context=job_context,
    spark_function=my_spark_scalar_fn,
    checkpoint_name="count_checkpoint",
)
def my_snowpark_scalar_fn(df: SnowparkDataFrame):
    return df.count()

df = job_context.snowpark_session.create_dataframe(
    [[1, 2], [3, 4]], schema=["a", "b"]
)
count = my_snowpark_scalar_fn(df)
```

### Pandera Snowpark Decorators

The decorators `@check_input_schema` and `@check_output_schema` allow for sampled schema validation of Snowpark dataframes in the input arguments or in the return value.

```python
from snowflake.snowpark_checkpoints.spark_migration import SamplingStrategy
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from pandera import DataFrameSchema
from typing import Optional

# Signature of the decorator
def check_input_schema(
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
    output_path: Optional[str] = None,
):
    ...

# Signature of the decorator
def check_output_schema(
    pandera_schema: DataFrameSchema,
    checkpoint_name: str,
    sample_frac: Optional[float] = 1.0,
    sample_number: Optional[int] = None,
    sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
    job_context: Optional[SnowparkJobContext] = None,
    output_path: Optional[str] = None,
):
    ...
```

- `pandera_schema`: Pandera schema to validate.
- `checkpoint_name`: Name of the checkpoint schema file or DataFrame.
- `sample_frac`: Fraction of the DataFrame to sample.
- `sample_number`: Number of rows to sample.
- `sampling_strategy`: Sampling strategy to use.
- `job_context`: Snowpark job context.
- `output_path`: Output path for the checkpoint report.

### Usage Example

#### Check Input Schema Example
```python
from pandas import DataFrame as PandasDataFrame
from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.checkpoint import check_input_schema
from numpy import int8

df = PandasDataFrame(
    {
        "COLUMN1": [1, 4, 0, 10, 9],
        "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
    }
)

in_schema = DataFrameSchema(
    {
        "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
        "COLUMN2": Column(float, Check(lambda x: x < -1.2, element_wise=True)),
    }
)

@check_input_schema(in_schema, "input_schema_checkpoint")
def preprocessor(dataframe: SnowparkDataFrame):
    dataframe = dataframe.withColumn(
        "COLUMN3", dataframe["COLUMN1"] + dataframe["COLUMN2"]
    )
    return dataframe

session = Session.builder.getOrCreate()
sp_dataframe = session.create_dataframe(df)

preprocessed_dataframe = preprocessor(sp_dataframe)
```

#### Check Input Schema Example
```python
from pandas import DataFrame as PandasDataFrame
from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.checkpoint import check_output_schema
from numpy import int8

df = PandasDataFrame(
    {
        "COLUMN1": [1, 4, 0, 10, 9],
        "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
    }
)

out_schema = DataFrameSchema(
    {
        "COLUMN1": Column(int8, Check.between(0, 10, include_max=True, include_min=True)),
        "COLUMN2": Column(float, Check.less_than_or_equal_to(-1.2)),
        "COLUMN3": Column(float, Check.less_than(10)),
    }
)

@check_output_schema(out_schema, "output_schema_checkpoint")
def preprocessor(dataframe: SnowparkDataFrame):
    return dataframe.with_column(
        "COLUMN3", dataframe["COLUMN1"] + dataframe["COLUMN2"]
    )

session = Session.builder.getOrCreate()
sp_dataframe = session.create_dataframe(df)

preprocessed_dataframe = preprocessor(sp_dataframe)
```

------

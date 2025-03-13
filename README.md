# snowpark-checkpoints

Snowpark Python / Spark Migration Testing Tools


[![Build and Test](https://github.com/snowflakedb/snowpark-checkpoints/actions/workflows/snowpark-checkpoints-all-tests.yml/badge.svg?branch=main)](https://github.com/snowflakedb/snowpark-checkpoints/actions/workflows/snowpark-checkpoints-all-tests.yml)
[![codecov](https://codecov.io/gh/snowflakedb/snowpark-checkpoints/branch/main/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowpark-checkpoints)
[![PyPi](https://img.shields.io/pypi/v/snowpark-checkpoints.svg)](https://pypi.org/project/snowpark-checkpoints)
[![License Apache-2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Codestyle Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

The **snowpark-checkpoints**  package is a testing library that will help you validate your migrated Snowpark code and discover any behavioral differences with the original Apache PySpark code.

[Source code][source code] | [Snowpark Checkpoints Developer guide][Snowpark Checkpoints Developer guide] | [Snowpark Checkpoints API references][Snowpark Checkpoints API references] 

---
##### This package is on Public Preview.
---
---
## Install the library 
```bash
pip install snowpark-checkpoints
```
This package requires PySpark to be installed in the same environment. If you do not have it, you can install PySpark alongside Snowpark Checkpoints by running the following command:
```bash
pip install "snowpark-checkpoints[pyspark]"
```
---

## Getting started

This bundle includes:
- **snowpark-checkpoints-collectors**: Extracts information from PySpark dataframes for validation against Snowpark dataframes.
- **snowpark-checkpoints-validators**: Validates Snowpark dataframes against predefined schemas and checkpoints.
- **snowpark-checkpoints-hypothesis**: Generates Snowpark dataframes using the Hypothesis library for testing and data generation.
- **snowpark-checkpoints-configuration**: Loads `checkpoint.json` and provides a model, working automatically with collectors and validators.
---
## snowpark-checkpoints-collectors


**snowpark-checkpoints-collector** package offers a function for extracting information from PySpark dataframes. We can then use that data to validate against the converted Snowpark dataframes to ensure that behavioral equivalence has been achieved.
## Features

- Schema inference collected data mode (Schema): This is the default mode, which leverages Pandera schema inference to obtain the metadata and checks that will be evaluated for the specified dataframe. This mode also collects custom data from columns of the DataFrame based on the PySpark type.
- DataFrame collected data mode (DataFrame): This mode collects the data of the PySpark dataframe. In this case, the mechanism saves all data of the given dataframe in parquet format. Using the default user Snowflake connection, it tries to upload the parquet files into the Snowflake temporal stage and create a table based on the information in the stage. The name of the file and the table is the same as the checkpoint.



## Functionalities

### Collect DataFrame Checkpoint



```python
from pyspark.sql import DataFrame as SparkDataFrame
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
from typing import Optional

# Signature of the function
def collect_dataframe_checkpoint(
    df: SparkDataFrame,
    checkpoint_name: str,
    sample: Optional[float] = None,
    mode: Optional[CheckpointMode] = None,
    output_path: Optional[str] = None,
) -> None:
    ...
```

- `df`: The input Spark dataframe to collect.
- `checkpoint_name`: Name of the checkpoint schema file or dataframe.
- `sample`: Fraction of DataFrame to sample for schema inference, defaults to 1.0.
- `mode`: The mode to execution the collection (Schema or Dataframe), defaults to CheckpointMode.Schema.
- `output_path`: The output path to save the checkpoint, defaults to current working directory.


## Usage Example

### Schema mode

```python
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode

spark_session = SparkSession.builder.getOrCreate()
sample_size = 1.0

pyspark_df = spark_session.createDataFrame(
    [("apple", 21), ("lemon", 34), ("banana", 50)], schema="fruit string, age integer"
)

collect_dataframe_checkpoint(
    pyspark_df,
    checkpoint_name="collect_checkpoint_mode_1",
    sample=sample_size,
    mode=CheckpointMode.SCHEMA,
)
```


### Dataframe mode

```python
from pyspark.sql import SparkSession
from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode
from pyspark.sql.types import StructType, StructField, ByteType, StringType, IntegerType 

spark_schema = StructType(
    [
        StructField("BYTE", ByteType(), True),
        StructField("STRING", StringType(), True),
        StructField("INTEGER", IntegerType(), True)
    ]
)

data = [(1, "apple", 21), (2, "lemon", 34), (3, "banana", 50)]

spark_session = SparkSession.builder.getOrCreate()
pyspark_df = spark_session.createDataFrame(data, schema=spark_schema).orderBy(
    "INTEGER"
)

collect_dataframe_checkpoint(
    pyspark_df,
    checkpoint_name="collect_checkpoint_mode_2",
    mode=CheckpointMode.DATAFRAME,
)
```

---

# snowpark-checkpoints-validators

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

---

# snowpark-checkpoints-hypothesis

**snowpark-checkpoints-hypothesis** is a [Hypothesis](https://hypothesis.readthedocs.io/en/latest/) extension for generating Snowpark DataFrames. This project provides strategies to facilitate testing and data generation for Snowpark DataFrames using the Hypothesis library.

## Installation

You can install this package using either **pip** or **conda**:

```shell
pip install snowpark-checkpoints-hypothesis
--or--
conda install snowpark-checkpoints-hypothesis
```

## Usage

The typical workflow for using the Hypothesis library to generate Snowpark dataframes is as follows:

1. Create a standard Python test function with the different assertions or conditions your code should satisfy for all inputs.
2. Add the Hypothesis `@given` decorator to your test function and pass the `dataframe_strategy` function as an argument.
3. Run the test. When the test is executed, Hypothesis will automatically provide the generated inputs as arguments to the test.

### Example 1: Generate Snowpark DataFrames from a JSON schema file

You can use the `dataframe_strategy` function to create Snowpark DataFrames from a JSON schema file generated by the `collect_dataframe_checkpoint` function of the [snowpark-checkpoints-collectors](https://pypi.org/project/snowpark-checkpoints-collectors/) package:

```python
from hypothesis import given
from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.snowpark import DataFrame, Session


@given(
    df=dataframe_strategy(
        schema="path/to/schema.json",
        session=Session.builder.getOrCreate(),
        size=10,
    )
)
def test_my_function(df: DataFrame):
    # Test your function here
    ...
```

### Example 2: Generate Snowpark DataFrames from a Pandera DataFrameSchema object

You can also use the `dataframe_strategy` function to create Snowpark DataFrames from a Pandera DataFrameSchema object:

```python
import pandera as pa
from hypothesis import given
from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.snowpark import DataFrame, Session

@given(
    df=dataframe_strategy(
        schema=pa.DataFrameSchema(
            {
                "A": pa.Column(pa.Int, checks=pa.Check.in_range(0, 10)),
                "B": pa.Column(pa.Bool),
            }
        ),
        session=Session.builder.getOrCreate(),
        size=10,
    )
)
def test_my_function(df: DataFrame):
    # Test your function here
    ...
```

## Development

### Set up a development environment

To set up a development environment, follow the steps below:

1. Create a virtual environment using **venv** or **conda**. Replace \<env-name\> with the name of your environment.

    Using **venv**:

    ```shell
    python3.11 -m venv <env-name>
    source <env-name>/bin/activate
    ```

    Using **conda**:

    ```shell
    conda create -n <env-name> python=3.11
    conda activate <env-name>
    ```

2. Configure your IDE to use the previously created virtual environment:

    * [Configuring a Python interpreter in PyCharm](https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html)
    * [Configuring a Python interpreter in VS Code](https://code.visualstudio.com/docs/python/environments#_manually-specify-an-interpreter)

3. Install the project dependencies:

    ```shell
    pip install hatch
    pip install -e .
    ```

### Running Tests

To run tests, run the following command.

```shell
hatch run test:check
```

---

# snowpark-checkpoints-configuration


**snowpark-checkpoints-configuration** is a module for loading `checkpoint.json` and provides a model. 
This module will work automatically with *snowpark-checkpoints-collector*  and *snowpark-checkpoints-validators*. This will try to read the configuration file from the current working directory.

## Usage

To explicit load a file, you can import  `CheckpointMetadata` and create an instance as shown below:

```python
from snowflake.snowpark_checkpoints_configuration import CheckpointMetadata

my_checkpoint_metadata = CheckpointMetadata("path/to/checkpoint.json")

checkpoint_model = my_checkpoint_metadata.get_checkpoint("my_checkpoint_name")
...
```
---

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
   pip install "snowpark-checkpoints"
4. First, run the PySpark demo:
   python demo_pyspark_pipeline.py
   This will generate the JSON schema files. Then, run the Snowpark demo:
   python demo_snowpark_pipeline.py

## Contributing
Please refer to [CONTRIBUTING.md][contributing].

------

[source code]: https://github.com/snowflakedb/snowpark-checkpoints
[Snowpark Checkpoints Developer guide]: https://docs.snowflake.com/en/developer-guide/snowpark/python/snowpark-checkpoints-library
[Snowpark Checkpoints API references]: https://docs.snowflake.com/en/developer-guide/snowpark-checkpoints-api/reference/latest/index
[contributing]: https://github.com/snowflakedb/snowpark-checkpoints/blob/main/CONTRIBUTING.md

# snowpark-checkpoints-collectors


---
##### This package is on Public Preview.
---

**snowpark-checkpoints-collector** package offers a function for extracting information from PySpark dataframes. We can then use that data to validate against the converted Snowpark dataframes to ensure that behavioral equivalence has been achieved.

---
## Install the library
```bash
pip install snowpark-checkpoints-collectors
```
This package requires PySpark to be installed in the same environment. If you do not have it, you can install PySpark alongside Snowpark Checkpoints by running the following command:
```bash
pip install "snowpark-checkpoints-collectors[pyspark]"
```
---

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

------

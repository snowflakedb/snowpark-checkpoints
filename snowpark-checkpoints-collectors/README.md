# Data Collection from Spark Pipelines

---
**NOTE**

This package is on Private Preview.

---

The `snowpark-checkpoints-collector` package can collect
schema and check information from a spark pipeline and
record those results into a set of JSON files corresponding to different intermediate dataframes. These files can be inspected manually
and handed over to teams implementing the snowpark pipeline. The `snowpark-checkpoints-collector` package is designed to have minimal
dependencies and the generated files are meant to be inspected by security
teams.

On the snowpark side the `snowpark-checkpoints` package can use these files to perform schema and data validation checks against snowpark dataframes at the same, intermediate logical "checkpoints".

## collect_dataframe_schema

```python
from snowflake.snowpark_checkpoints_collector import collect_dataframe_schema;
from snowflake.snowpark_checkpoints_collector.collection_common import (
    SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR,
    CheckpointMode,
)

collect_dataframe_schema(df:SparkDataFrame,
                              checkpoint_name,
                              mode=CheckpointMode.DATAFRAME
                              sample=0.1)
```

- df - the spark data frame to collect the schema from
- checkpoint_name - the name of the "checkpoint". Generated JSON files
  will have the name "snowpark-[checkpoint_name]-schema.json"
- mode - will define if collect the dataframe schema using Pandera or export the Dataframe using Parquet files 
  - CheckpointMode.SCHEMA (Default): For inferred schema using Pandera.
  - CheckpointMode.DATAFRAME: To collect Parquet files
- sample - sample size of the spark data frame to use to generate the schema. The Default value is 1.0 which means the entire DataFrame.

## License

This project is licensed under the  Apache License Version 2.0. See the [LICENSE](LICENSE) file for more details.

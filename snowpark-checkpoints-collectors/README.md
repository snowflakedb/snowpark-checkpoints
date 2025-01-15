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

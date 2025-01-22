#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_FORMAT_KEY,
    COLUMN_MAX_KEY,
    COLUMN_MIN_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


FORMAT = "%Y-%m-%d"


class DateColumnCollector(ColumnCollectorBase):

    """Class for collect a date type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: SparkDataFrame
    ) -> None:
        """Init DateColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        select_result = self.values.select(
            spark_min(spark_col(self.name)).alias(COLUMN_MIN_KEY),
            spark_max(spark_col(self.name)).alias(COLUMN_MAX_KEY),
        ).collect()[0]

        min_value = str(select_result[COLUMN_MIN_KEY])
        max_value = str(select_result[COLUMN_MAX_KEY])

        custom_data_dict = {
            COLUMN_MIN_KEY: min_value,
            COLUMN_MAX_KEY: max_value,
            COLUMN_FORMAT_KEY: FORMAT,
        }

        return custom_data_dict

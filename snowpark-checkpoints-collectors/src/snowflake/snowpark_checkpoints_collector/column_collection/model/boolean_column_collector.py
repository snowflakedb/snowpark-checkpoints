#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import lit as spark_lit
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_FALSE_COUNT_KEY,
    COLUMN_TRUE_COUNT_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class BooleanColumnCollector(ColumnCollectorBase):

    """Class for collect a boolean type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: SparkDataFrame
    ) -> None:
        """Init BooleanColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        true_count = self.values.where(
            self.values[self.name] == spark_lit(True)
        ).count()
        false_count = self.values.where(
            self.values[self.name] == spark_lit(False)
        ).count()

        custom_data_dict = {
            COLUMN_TRUE_COUNT_KEY: true_count,
            COLUMN_FALSE_COUNT_KEY: false_count,
        }

        return custom_data_dict

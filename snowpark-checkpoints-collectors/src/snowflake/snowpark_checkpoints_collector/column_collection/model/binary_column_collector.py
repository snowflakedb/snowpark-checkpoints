#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from statistics import mean

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import coalesce as spark_coalesce
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import length as spark_length
from pyspark.sql.functions import lit as spark_lit
from pyspark.sql.functions import to_binary as spark_to_binary
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_IS_UNIQUE_SIZE_KEY,
    COLUMN_MAX_SIZE_KEY,
    COLUMN_MEAN_SIZE_KEY,
    COLUMN_MIN_SIZE_KEY,
    COLUMN_SIZE_COLLECTION_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class BinaryColumnCollector(ColumnCollectorBase):

    """Class for collect a binary type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: SparkDataFrame
    ) -> None:
        """Init BinaryColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_values)
        self._binary_size_collection = self._compute_binary_size_collection()

    def get_custom_data(self) -> dict[str, any]:
        max_size_value = max(self._binary_size_collection)
        min_size_value = min(self._binary_size_collection)
        mean_size_value = mean(self._binary_size_collection)
        all_elements_have_same_size = max_size_value == min_size_value

        custom_data_dict = {
            COLUMN_MAX_SIZE_KEY: max_size_value,
            COLUMN_MIN_SIZE_KEY: min_size_value,
            COLUMN_MEAN_SIZE_KEY: mean_size_value,
            COLUMN_IS_UNIQUE_SIZE_KEY: all_elements_have_same_size,
        }

        return custom_data_dict

    def _compute_binary_size_collection(self) -> list[int]:
        select_result = self.values.select(
            spark_length(
                spark_coalesce(spark_col(self.name), spark_to_binary(spark_lit(b"")))
            ).alias(COLUMN_SIZE_COLLECTION_KEY)
        ).collect()

        size_collection = []
        for row in select_result:
            size = row[COLUMN_SIZE_COLLECTION_KEY]
            size_collection.append(size)

        return size_collection

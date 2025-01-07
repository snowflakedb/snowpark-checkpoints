#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from statistics import mean

from pandas import Series
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_IS_UNIQUE_SIZE_KEY,
    COLUMN_MAX_SIZE_KEY,
    COLUMN_MEAN_SIZE_KEY,
    COLUMN_MIN_SIZE_KEY,
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
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: Series
    ) -> None:
        """Init BinaryColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pandas.Series): the column values as Pandas.Series.

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
        size_collection = []
        for binary in self.values:
            if binary is None:
                continue

            length = len(binary)
            size_collection.append(length)

        return size_collection

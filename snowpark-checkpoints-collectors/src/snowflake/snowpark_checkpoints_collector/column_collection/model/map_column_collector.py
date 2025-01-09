#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from statistics import mean

from pandas import Series
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_ALLOW_NULL_KEY,
    COLUMN_IS_UNIQUE_SIZE_KEY,
    COLUMN_KEY_TYPE_KEY,
    COLUMN_MAX_SIZE_KEY,
    COLUMN_MEAN_SIZE_KEY,
    COLUMN_MIN_SIZE_KEY,
    COLUMN_NULL_VALUE_PROPORTION_KEY,
    COLUMN_VALUE_TYPE_KEY,
    KEY_TYPE_KEY,
    VALUE_CONTAINS_NULL_KEY,
    VALUE_TYPE_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class MapColumnCollector(ColumnCollectorBase):

    """Class for collect a map type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: Series
    ) -> None:
        """Init MapColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, struct_field, clm_values)
        self._map_size_collection = self._compute_map_size_collection()

    def get_custom_data(self) -> dict[str, any]:
        data_type_dict = self.struct_field.dataType.jsonValue()
        key_type = data_type_dict[KEY_TYPE_KEY]
        value_type = data_type_dict[VALUE_TYPE_KEY]
        allow_null = data_type_dict[VALUE_CONTAINS_NULL_KEY]
        null_value_proportion = (
            self._compute_null_value_proportion() if allow_null else 0.0
        )
        array_max_size = max(self._map_size_collection)
        array_min_size = min(self._map_size_collection)
        array_mean_size = mean(self._map_size_collection)
        all_array_have_same_size = array_max_size == array_min_size

        custom_data_dict = {
            COLUMN_KEY_TYPE_KEY: key_type,
            COLUMN_VALUE_TYPE_KEY: value_type,
            COLUMN_ALLOW_NULL_KEY: allow_null,
            COLUMN_NULL_VALUE_PROPORTION_KEY: null_value_proportion,
            COLUMN_MAX_SIZE_KEY: array_max_size,
            COLUMN_MIN_SIZE_KEY: array_min_size,
            COLUMN_MEAN_SIZE_KEY: array_mean_size,
            COLUMN_IS_UNIQUE_SIZE_KEY: all_array_have_same_size,
        }

        return custom_data_dict

    def _compute_map_size_collection(self) -> list[int]:
        size_collection = []
        for mapp in self.values:
            if mapp is None:
                continue

            length = len(mapp)
            size_collection.append(length)

        return size_collection

    def _compute_null_value_proportion(self) -> float:
        null_counter = 0
        for mapp in self.values:
            if mapp is None:
                continue

            values_collection = list(mapp.values())
            null_counter += values_collection.count(None)

        total_values = sum(self._map_size_collection)
        null_value_proportion = (null_counter / total_values) * 100
        return null_value_proportion

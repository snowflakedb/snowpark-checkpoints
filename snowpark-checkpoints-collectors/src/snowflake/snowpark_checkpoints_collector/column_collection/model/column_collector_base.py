#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod

from pandas import Series
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_COUNT_KEY,
    COLUMN_IS_NULLABLE_KEY,
    COLUMN_NAME_KEY,
    COLUMN_ROWS_NOT_NULL_COUNT_KEY,
    COLUMN_ROWS_NULL_COUNT_KEY,
    COLUMN_TYPE_KEY,
)


class ColumnCollectorBase(ABC):

    """Base class for column collector based on type.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: Series
    ) -> None:
        """Init ColumnCollectorBase.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        self.name = clm_name
        self.type = struct_field.dataType.typeName()
        self.struct_field = struct_field
        self.values = clm_values

    @abstractmethod
    def get_custom_data(self) -> dict[str, any]:
        """Get the custom data of the column.

        Returns:
            dict[str, any]: The data collected.

        """
        pass

    def _get_common_data(self) -> dict[str, any]:
        column_size = len(self.values)
        rows_not_null_count = self.values.count().item()
        rows_null_count = column_size - rows_not_null_count

        common_data_dict = {
            COLUMN_NAME_KEY: self.name,
            COLUMN_TYPE_KEY: self.type,
            COLUMN_IS_NULLABLE_KEY: self.struct_field.nullable,
            COLUMN_COUNT_KEY: column_size,
            COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
            COLUMN_ROWS_NULL_COUNT_KEY: rows_null_count,
        }

        return common_data_dict

    def get_data(self) -> dict[str, any]:
        """Get the data collected of the column.

        Returns:
            dict[str, any]: The data collected.

        """
        common_data = self._get_common_data()
        custom_data = self.get_custom_data()
        column_data = dict(common_data | custom_data)
        return column_data

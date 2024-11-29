#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from abc import ABC, abstractmethod

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_COUNT_KEY,
    COLUMN_NAME_KEY,
    COLUMN_NULL_COUNT_KEY,
    COLUMN_ROWS_NOT_NULL_COUNT_KEY,
    COLUMN_TYPE_KEY,
)


class ColumnCollectorBase(ABC):
    def __init__(self, clm_name, clm_type, clm_values) -> None:
        self.name = clm_name
        self.type = clm_type
        self.values = clm_values

    @abstractmethod
    def get_custom_data(self) -> dict[str, any]:
        pass

    def get_common_data(self) -> dict[str, any]:
        column_size = len(self.values)
        rows_not_null_count = self.values.count().item()
        rows_null_count = column_size - rows_not_null_count

        common_data_dict = {
            COLUMN_NAME_KEY: self.name,
            COLUMN_TYPE_KEY: self.type,
            COLUMN_COUNT_KEY: column_size,
            COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
            COLUMN_NULL_COUNT_KEY: rows_null_count,
        }

        return common_data_dict

    def get_data(self) -> dict[str, any]:
        common_data = self.get_common_data()
        custom_data = self.get_custom_data()
        column_data = dict(common_data | custom_data)
        return column_data

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_FALSE_COUNT_KEY,
    COLUMN_TRUE_COUNT_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class BooleanColumnCollector(ColumnCollectorBase):
    def __init__(self, clm_name, clm_type, clm_values) -> None:
        super().__init__(clm_name, clm_type, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        rows_count = self.values.count().item()
        true_count = self.values.where(self.values).count().item()
        false_count = rows_count - true_count

        custom_data_dict = {
            COLUMN_TRUE_COUNT_KEY: true_count,
            COLUMN_FALSE_COUNT_KEY: false_count,
        }

        return custom_data_dict

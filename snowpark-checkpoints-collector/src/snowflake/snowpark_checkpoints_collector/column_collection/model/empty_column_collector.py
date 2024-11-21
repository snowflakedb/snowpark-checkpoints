#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class EmptyColumnCollector(ColumnCollectorBase):
    def __init__(self, clm_name, clm_type, clm_values) -> None:
        super().__init__(clm_name, clm_type, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        return {}

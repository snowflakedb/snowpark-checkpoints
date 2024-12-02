#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark_checkpoints_collector.collection_common import (
    STRING_COLUMN_TYPE,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class StringColumnCollector(ColumnCollectorBase):

    """Class for collect a string type column.

    Attributes:
        name (str): the name of the column.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(self, clm_name, clm_values) -> None:
        """Init StringColumnCollector.

        Args:
            clm_name (str): the name of the column.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, STRING_COLUMN_TYPE, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        return {}

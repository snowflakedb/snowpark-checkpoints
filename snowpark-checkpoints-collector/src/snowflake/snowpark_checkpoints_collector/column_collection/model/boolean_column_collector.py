#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandas import Series

from snowflake.snowpark_checkpoints_collector.collection_common import (
    BOOLEAN_COLUMN_TYPE,
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
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(self, clm_name: str, clm_values: Series) -> None:
        """Init BooleanColumnCollector.

        Args:
            clm_name (str): the name of the column.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, BOOLEAN_COLUMN_TYPE, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        rows_count = self.values.count().item()
        true_count = self.values.where(self.values).count().item()
        false_count = rows_count - true_count

        custom_data_dict = {
            COLUMN_TRUE_COUNT_KEY: true_count,
            COLUMN_FALSE_COUNT_KEY: false_count,
        }

        return custom_data_dict

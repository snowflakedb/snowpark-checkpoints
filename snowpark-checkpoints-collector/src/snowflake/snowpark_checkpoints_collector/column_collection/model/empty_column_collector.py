#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandas import Series

from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class EmptyColumnCollector(ColumnCollectorBase):

    """Class for collect an empty column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(self, clm_name: str, clm_type: str, clm_values: Series) -> None:
        """Init EmptyColumnCollector.

        Args:
            clm_name (str): the name of the column.
            clm_type (str): the type of the column.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, clm_type, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        return {}

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#


from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_FORMAT_KEY,
    COLUMN_MAX_KEY,
    COLUMN_MIN_KEY,
    TIMESTAMP_COLUMN_TYPE,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


FORMAT = "%Y-%m-%dH:%M:%S"


class TimestampColumnCollector(ColumnCollectorBase):

    """Class for collect a timestamp type column.

    Attributes:
        name (str): the name of the column.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(self, clm_name, clm_values) -> None:
        """Init TimestampColumnCollector.

        Args:
            clm_name (str): the name of the column.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, TIMESTAMP_COLUMN_TYPE, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        min_value = str(self.values.min())
        max_value = str(self.values.max())

        custom_data_dict = {
            COLUMN_MIN_KEY: min_value,
            COLUMN_MAX_KEY: max_value,
            COLUMN_FORMAT_KEY: FORMAT,
        }

        return custom_data_dict

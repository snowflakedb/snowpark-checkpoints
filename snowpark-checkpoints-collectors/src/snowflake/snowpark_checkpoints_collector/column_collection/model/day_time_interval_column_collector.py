#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandas import Series
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_MAX_KEY,
    COLUMN_MIN_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class DayTimeIntervalColumnCollector(ColumnCollectorBase):

    """Class for collect a date time interval type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: Series
    ) -> None:
        """Init DayTimeIntervalColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, struct_field, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        min_value = str(self.values.min())
        max_value = str(self.values.max())

        custom_data_dict = {COLUMN_MIN_KEY: min_value, COLUMN_MAX_KEY: max_value}

        return custom_data_dict

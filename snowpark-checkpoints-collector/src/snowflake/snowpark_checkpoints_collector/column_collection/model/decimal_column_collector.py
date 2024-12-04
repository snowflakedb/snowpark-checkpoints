#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandas import Series

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_DECIMAL_PRECISION_KEY,
    COLUMN_MAX_KEY,
    COLUMN_MEAN_KEY,
    COLUMN_MIN_KEY,
    DECIMAL_COLUMN_TYPE,
    get_decimal_token,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model import (
    ColumnCollectorBase,
)


class DecimalColumnCollector(ColumnCollectorBase):

    """Class for collect a decimal type column.

    Attributes:
        name (str): the name of the column.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(self, clm_name: str, clm_values: Series) -> None:
        """Init DecimalColumnCollector.

        Args:
            clm_name (str): the name of the column.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, DECIMAL_COLUMN_TYPE, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        min_value = str(self.values.min())
        max_value = str(self.values.max())
        mean_value = str(self.values.mean().item())
        decimal_precision = self._compute_decimal_precision()

        custom_data_dict = {
            COLUMN_MIN_KEY: min_value,
            COLUMN_MAX_KEY: max_value,
            COLUMN_MEAN_KEY: mean_value,
            COLUMN_DECIMAL_PRECISION_KEY: decimal_precision,
        }

        return custom_data_dict

    def _compute_decimal_precision(self) -> int:
        decimal_part_index = 1
        decimal_token = get_decimal_token()
        value = self.values[0]
        value_str = str(value)
        value_split_by_token = value_str.split(decimal_token)
        decimal_part = value_split_by_token[decimal_part_index]
        decimal_digits_counted = len(decimal_part)
        return decimal_digits_counted

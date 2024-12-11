#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandas import Series

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_DECIMAL_PRECISION_KEY,
    COLUMN_MARGIN_ERROR_KEY,
    COLUMN_MAX_KEY,
    COLUMN_MEAN_KEY,
    COLUMN_MIN_KEY,
    INTEGER_TYPE_COLLECTION,
    get_decimal_token,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class NumericColumnCollector(ColumnCollectorBase):

    """Class for collect an empty column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        values (pandas.Series): the column values as Pandas.Series.
        is_integer (boolean): a flag to indicate if values are integer or do not.

    """

    def __init__(self, clm_name: str, clm_type: str, clm_values: Series) -> None:
        """Init NumericColumnCollector.

        Args:
            clm_name (str): the name of the column.
            clm_type (str): the type of the column.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, clm_type, clm_values)
        self.is_integer = self.type in INTEGER_TYPE_COLLECTION

    def get_custom_data(self) -> dict[str, any]:
        min_value = self.values.min().item()
        max_value = self.values.max().item()
        mean_value = self.values.mean().item()
        decimal_precision = self._compute_decimal_precision()
        margin_error = self.values.std().item()

        custom_data_dict = {
            COLUMN_MIN_KEY: min_value,
            COLUMN_MAX_KEY: max_value,
            COLUMN_MEAN_KEY: mean_value,
            COLUMN_DECIMAL_PRECISION_KEY: decimal_precision,
            COLUMN_MARGIN_ERROR_KEY: margin_error,
        }

        return custom_data_dict

    def _compute_decimal_precision(self) -> int:
        if self.is_integer:
            return 0

        decimal_part_index = 1
        decimal_token = get_decimal_token()
        max_decimal_digits_counted = 0

        for value in self.values:
            value_str = str(value)
            value_split_by_token = value_str.split(decimal_token)
            decimal_part = value_split_by_token[decimal_part_index]
            decimal_digits_counted = len(decimal_part)
            max_decimal_digits_counted = max(
                decimal_digits_counted, max_decimal_digits_counted
            )

        return max_decimal_digits_counted

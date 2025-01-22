#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_mean
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import stddev as spark_sdt
from pyspark.sql.types import StructField

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
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.
        is_integer (boolean): a flag to indicate if values are integer or do not.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: SparkDataFrame
    ) -> None:
        """Init NumericColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_values)
        self.is_integer = self.type in INTEGER_TYPE_COLLECTION

    def get_custom_data(self) -> dict[str, any]:
        select_result = self.values.select(
            spark_min(spark_col(self.name)).alias(COLUMN_MIN_KEY),
            spark_max(spark_col(self.name)).alias(COLUMN_MAX_KEY),
            spark_mean(spark_col(self.name)).alias(COLUMN_MEAN_KEY),
            spark_sdt(spark_col(self.name)).alias(COLUMN_MARGIN_ERROR_KEY),
        ).collect()[0]

        min_value = select_result[COLUMN_MIN_KEY]
        max_value = select_result[COLUMN_MAX_KEY]
        mean_value = select_result[COLUMN_MEAN_KEY]
        decimal_precision = self._compute_decimal_precision()
        margin_error = select_result[COLUMN_MARGIN_ERROR_KEY]

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

        row_collection = self.values.dropna().collect()
        for row in row_collection:
            value = row[0]
            value_str = str(value)
            value_split_by_token = value_str.split(decimal_token)
            decimal_part = value_split_by_token[decimal_part_index]
            decimal_digits_counted = len(decimal_part)
            max_decimal_digits_counted = max(
                decimal_digits_counted, max_decimal_digits_counted
            )

        return max_decimal_digits_counted

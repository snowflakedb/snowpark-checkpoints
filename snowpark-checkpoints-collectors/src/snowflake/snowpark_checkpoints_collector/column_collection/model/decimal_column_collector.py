# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import mean as spark_mean
from pyspark.sql.functions import min as spark_min
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_DECIMAL_PRECISION_KEY,
    COLUMN_MAX_KEY,
    COLUMN_MEAN_KEY,
    COLUMN_MIN_KEY,
    get_decimal_token,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class DecimalColumnCollector(ColumnCollectorBase):

    """Class for collect a decimal type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        column_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_df: SparkDataFrame
    ) -> None:
        """Init DecimalColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_df)

    def get_custom_data(self) -> dict[str, any]:
        select_result = self.column_df.select(
            spark_min(spark_col(self.name)).alias(COLUMN_MIN_KEY),
            spark_max(spark_col(self.name)).alias(COLUMN_MAX_KEY),
            spark_mean(spark_col(self.name)).alias(COLUMN_MEAN_KEY),
        ).collect()[0]

        min_value = str(select_result[COLUMN_MIN_KEY])
        max_value = str(select_result[COLUMN_MAX_KEY])
        mean_value = str(select_result[COLUMN_MEAN_KEY])
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
        value = self.column_df.dropna().collect()[0][0]
        value_str = str(value)
        value_split_by_token = value_str.split(decimal_token)
        if len(value_split_by_token) == 1:
            return 0

        decimal_part = value_split_by_token[decimal_part_index]
        decimal_digits_counted = len(decimal_part)
        return decimal_digits_counted

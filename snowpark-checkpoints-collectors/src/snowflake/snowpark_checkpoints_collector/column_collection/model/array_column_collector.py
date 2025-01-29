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
from statistics import mean

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import array as spark_array
from pyspark.sql.functions import coalesce as spark_coalesce
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import explode as spark_explode
from pyspark.sql.functions import size as spark_size
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_ALLOW_NULL_KEY,
    COLUMN_IS_UNIQUE_SIZE_KEY,
    COLUMN_MAX_SIZE_KEY,
    COLUMN_MEAN_SIZE_KEY,
    COLUMN_MIN_SIZE_KEY,
    COLUMN_NULL_VALUE_PROPORTION_KEY,
    COLUMN_SIZE_KEY,
    COLUMN_VALUE_KEY,
    COLUMN_VALUE_TYPE_KEY,
    CONTAINS_NULL_KEY,
    ELEMENT_TYPE_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class ArrayColumnCollector(ColumnCollectorBase):

    """Class for collect an array type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        column_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_df: SparkDataFrame
    ) -> None:
        """Init ArrayColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_df)
        self._array_size_collection = self._compute_array_size_collection()

    def get_custom_data(self) -> dict[str, any]:
        data_type_dict = self.struct_field.dataType.jsonValue()
        array_type = data_type_dict[ELEMENT_TYPE_KEY]
        allow_null = data_type_dict[CONTAINS_NULL_KEY]
        null_value_proportion = (
            self._compute_null_value_proportion() if allow_null else 0.0
        )
        array_max_size = max(self._array_size_collection)
        array_min_size = min(self._array_size_collection)
        array_mean_size = mean(self._array_size_collection)
        all_array_have_same_size = array_max_size == array_min_size

        custom_data_dict = {
            COLUMN_VALUE_TYPE_KEY: array_type,
            COLUMN_ALLOW_NULL_KEY: allow_null,
            COLUMN_NULL_VALUE_PROPORTION_KEY: null_value_proportion,
            COLUMN_MAX_SIZE_KEY: array_max_size,
            COLUMN_MIN_SIZE_KEY: array_min_size,
            COLUMN_MEAN_SIZE_KEY: array_mean_size,
            COLUMN_IS_UNIQUE_SIZE_KEY: all_array_have_same_size,
        }

        return custom_data_dict

    def _compute_array_size_collection(self) -> list[int]:
        select_result = self.column_df.select(
            spark_size(spark_coalesce(spark_col(self.name), spark_array([]))).alias(
                COLUMN_SIZE_KEY
            )
        ).collect()

        size_collection = [row[COLUMN_SIZE_KEY] for row in select_result]

        return size_collection

    def _compute_null_value_proportion(self) -> float:
        select_result = self.column_df.select(
            spark_explode(spark_col(self.name)).alias(COLUMN_VALUE_KEY)
        )

        null_counter = select_result.where(spark_col(COLUMN_VALUE_KEY).isNull()).count()

        total_values = sum(self._array_size_collection)
        null_value_proportion = (null_counter / total_values) * 100
        return null_value_proportion

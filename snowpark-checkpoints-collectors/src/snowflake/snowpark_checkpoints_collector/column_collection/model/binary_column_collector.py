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
from pyspark.sql.functions import coalesce as spark_coalesce
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import length as spark_length
from pyspark.sql.functions import lit as spark_lit
from pyspark.sql.functions import to_binary as spark_to_binary
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_IS_UNIQUE_SIZE_KEY,
    COLUMN_MAX_SIZE_KEY,
    COLUMN_MEAN_SIZE_KEY,
    COLUMN_MIN_SIZE_KEY,
    COLUMN_SIZE_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class BinaryColumnCollector(ColumnCollectorBase):

    """Class for collect a binary type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        column_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_df: SparkDataFrame
    ) -> None:
        """Init BinaryColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_df)
        self._binary_size_collection = self._compute_binary_size_collection()

    def get_custom_data(self) -> dict[str, any]:
        max_size_value = max(self._binary_size_collection)
        min_size_value = min(self._binary_size_collection)
        mean_size_value = mean(self._binary_size_collection)
        all_elements_have_same_size = max_size_value == min_size_value

        custom_data_dict = {
            COLUMN_MAX_SIZE_KEY: max_size_value,
            COLUMN_MIN_SIZE_KEY: min_size_value,
            COLUMN_MEAN_SIZE_KEY: mean_size_value,
            COLUMN_IS_UNIQUE_SIZE_KEY: all_elements_have_same_size,
        }

        return custom_data_dict

    def _compute_binary_size_collection(self) -> list[int]:
        select_result = self.column_df.select(
            spark_length(
                spark_coalesce(spark_col(self.name), spark_to_binary(spark_lit(b"")))
            ).alias(COLUMN_SIZE_KEY)
        ).collect()

        size_collection = [row[COLUMN_SIZE_KEY] for row in select_result]

        return size_collection

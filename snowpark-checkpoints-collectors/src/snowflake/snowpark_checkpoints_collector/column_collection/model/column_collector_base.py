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

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_COUNT_KEY,
    COLUMN_IS_NULLABLE_KEY,
    COLUMN_NAME_KEY,
    COLUMN_ROWS_NOT_NULL_COUNT_KEY,
    COLUMN_ROWS_NULL_COUNT_KEY,
    COLUMN_TYPE_KEY,
)


class ColumnCollectorBase(ABC):

    """Base class for column collector based on type.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        column_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_df: SparkDataFrame
    ) -> None:
        """Init ColumnCollectorBase.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        self.name = clm_name
        self.type = struct_field.dataType.typeName()
        self.struct_field = struct_field
        self.column_df = clm_df

    @abstractmethod
    def get_custom_data(self) -> dict[str, any]:
        """Get the custom data of the column.

        Returns:
            dict[str, any]: The data collected.

        """
        pass

    def _get_common_data(self) -> dict[str, any]:
        column_size = self.column_df.count()
        rows_not_null_count = self.column_df.dropna().count()
        rows_null_count = column_size - rows_not_null_count

        common_data_dict = {
            COLUMN_NAME_KEY: self.name,
            COLUMN_TYPE_KEY: self.type,
            COLUMN_IS_NULLABLE_KEY: self.struct_field.nullable,
            COLUMN_COUNT_KEY: column_size,
            COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
            COLUMN_ROWS_NULL_COUNT_KEY: rows_null_count,
        }

        return common_data_dict

    def get_data(self) -> dict[str, any]:
        """Get the data collected of the column.

        Returns:
            dict[str, any]: The data collected.

        """
        common_data = self._get_common_data()
        custom_data = self.get_custom_data()
        column_data = dict(common_data | custom_data)
        return column_data

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
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_FALSE_COUNT_KEY,
    COLUMN_TRUE_COUNT_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


TRUE_KEY = "True"
FALSE_KEY = "False"
NONE_KEY = "None"


class BooleanColumnCollector(ColumnCollectorBase):

    """Class for collect a boolean type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        column_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_df: SparkDataFrame
    ) -> None:
        """Init BooleanColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_df)

    def get_custom_data(self) -> dict[str, any]:
        count_dict = self._get_count_dict()

        custom_data_dict = {
            COLUMN_TRUE_COUNT_KEY: count_dict.get(TRUE_KEY, 0),
            COLUMN_FALSE_COUNT_KEY: count_dict.get(FALSE_KEY, 0),
        }

        return custom_data_dict

    def _get_count_dict(self) -> dict[str, int]:
        select_result = self.column_df.groupby(self.name).count().collect()
        count_dict = {str(row[0]): row[1] for row in select_result}
        return count_dict

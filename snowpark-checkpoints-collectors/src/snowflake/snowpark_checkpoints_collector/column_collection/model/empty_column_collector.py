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
    ARRAY_COLUMN_TYPE,
    COLUMN_ALLOW_NULL_KEY,
    COLUMN_KEY_TYPE_KEY,
    COLUMN_METADATA_KEY,
    COLUMN_VALUE_TYPE_KEY,
    CONTAINS_NULL_KEY,
    ELEMENT_TYPE_KEY,
    FIELD_METADATA_KEY,
    FIELDS_KEY,
    KEY_TYPE_KEY,
    MAP_COLUMN_TYPE,
    STRUCT_COLUMN_TYPE,
    VALUE_CONTAINS_NULL_KEY,
    VALUE_TYPE_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class EmptyColumnCollector(ColumnCollectorBase):

    """Class for collect an empty column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        column_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_df: SparkDataFrame
    ) -> None:
        """Init EmptyColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_df (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        """
        super().__init__(clm_name, struct_field, clm_df)

    def get_custom_data(self) -> dict[str, any]:
        custom_data = {}
        data_type_dict = self.struct_field.dataType.jsonValue()
        if self.type == ARRAY_COLUMN_TYPE:
            custom_data[COLUMN_VALUE_TYPE_KEY] = data_type_dict[ELEMENT_TYPE_KEY]
            custom_data[COLUMN_ALLOW_NULL_KEY] = data_type_dict[CONTAINS_NULL_KEY]

        elif self.type == MAP_COLUMN_TYPE:
            custom_data[COLUMN_KEY_TYPE_KEY] = data_type_dict[KEY_TYPE_KEY]
            custom_data[COLUMN_VALUE_TYPE_KEY] = data_type_dict[VALUE_TYPE_KEY]
            custom_data[COLUMN_ALLOW_NULL_KEY] = data_type_dict[VALUE_CONTAINS_NULL_KEY]

        elif self.type == STRUCT_COLUMN_TYPE:
            field_metadata_collection = []
            struct_field_json = self.struct_field.dataType.jsonValue()
            for field in struct_field_json[FIELDS_KEY]:
                del field[FIELD_METADATA_KEY]
                field_metadata_collection.append(field)
            custom_data[COLUMN_METADATA_KEY] = field_metadata_collection

        else:
            custom_data = {}

        return custom_data

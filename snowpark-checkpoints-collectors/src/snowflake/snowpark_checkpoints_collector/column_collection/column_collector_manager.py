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
    BINARY_COLUMN_TYPE,
    BOOLEAN_COLUMN_TYPE,
    BYTE_COLUMN_TYPE,
    DATE_COLUMN_TYPE,
    DAYTIMEINTERVAL_COLUMN_TYPE,
    DECIMAL_COLUMN_TYPE,
    DOUBLE_COLUMN_TYPE,
    FLOAT_COLUMN_TYPE,
    INTEGER_COLUMN_TYPE,
    LONG_COLUMN_TYPE,
    MAP_COLUMN_TYPE,
    NULL_COLUMN_TYPE,
    SHORT_COLUMN_TYPE,
    STRING_COLUMN_TYPE,
    STRUCT_COLUMN_TYPE,
    TIMESTAMP_COLUMN_TYPE,
    TIMESTAMP_NTZ_COLUMN_TYPE,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model import (
    ArrayColumnCollector,
    BinaryColumnCollector,
    BooleanColumnCollector,
    DateColumnCollector,
    DayTimeIntervalColumnCollector,
    DecimalColumnCollector,
    EmptyColumnCollector,
    MapColumnCollector,
    NullColumnCollector,
    NumericColumnCollector,
    StringColumnCollector,
    StructColumnCollector,
    TimestampColumnCollector,
    TimestampNTZColumnCollector,
)


def collector_register(cls):
    """Decorate a class with the collection type mechanism.

    Args:
        cls: The class to decorate.

    Returns:
        The class to decorate.

    """
    cls._collectors = {}
    for method_name in dir(cls):
        method = getattr(cls, method_name)
        if hasattr(method, "_column_type"):
            col_type_collection = method._column_type
            for col_type in col_type_collection:
                cls._collectors[col_type] = method_name
    return cls


def column_register(*args):
    """Decorate a method to register it in the collection mechanism based on column type.

    Args:
        args: the column type to register.

    Returns:
        The wrapper.

    """

    def wrapper(func):
        has_arguments = len(args) > 0
        if has_arguments:
            func._column_type = args
        return func

    return wrapper


@collector_register
class ColumnCollectorManager:

    """Manage class for column collector based on type."""

    def collect_column(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        """Collect the data of the column based on the column type.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        Returns:
            dict[str, any]: The data collected.

        """
        clm_type = struct_field.dataType.typeName()
        if clm_type not in self._collectors:
            return {}

        func_name = self._collectors[clm_type]
        func = getattr(self, func_name)
        data = func(clm_name, struct_field, values)
        return data

    @column_register(ARRAY_COLUMN_TYPE)
    def _collect_array_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = ArrayColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(BINARY_COLUMN_TYPE)
    def _collect_binary_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = BinaryColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(BOOLEAN_COLUMN_TYPE)
    def _collect_boolean_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = BooleanColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(DATE_COLUMN_TYPE)
    def _collect_date_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = DateColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(DAYTIMEINTERVAL_COLUMN_TYPE)
    def _collect_day_time_interval_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = DayTimeIntervalColumnCollector(
            clm_name, struct_field, values
        )
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(DECIMAL_COLUMN_TYPE)
    def _collect_decimal_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = DecimalColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(MAP_COLUMN_TYPE)
    def _collect_map_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = MapColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(NULL_COLUMN_TYPE)
    def _collect_null_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = NullColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(
        BYTE_COLUMN_TYPE,
        SHORT_COLUMN_TYPE,
        INTEGER_COLUMN_TYPE,
        LONG_COLUMN_TYPE,
        FLOAT_COLUMN_TYPE,
        DOUBLE_COLUMN_TYPE,
    )
    def _collect_numeric_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = NumericColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(STRING_COLUMN_TYPE)
    def _collect_string_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = StringColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(STRUCT_COLUMN_TYPE)
    def _collect_struct_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = StructColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(TIMESTAMP_COLUMN_TYPE)
    def _collect_timestamp_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = TimestampColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(TIMESTAMP_NTZ_COLUMN_TYPE)
    def _collect_timestampntz_type_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        column_collector = TimestampNTZColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

    def collect_empty_custom_data(
        self, clm_name: str, struct_field: StructField, values: SparkDataFrame
    ) -> dict[str, any]:
        """Collect the data of a empty column.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            values (pyspark.sql.DataFrame): the column values as PySpark DataFrame.

        Returns:
            dict[str, any]: The data collected.

        """
        column_collector = EmptyColumnCollector(clm_name, struct_field, values)
        collected_data = column_collector.get_data()
        return collected_data

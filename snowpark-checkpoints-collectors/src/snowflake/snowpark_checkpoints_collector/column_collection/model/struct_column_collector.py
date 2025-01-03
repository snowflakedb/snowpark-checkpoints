#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from pandas import Series
from pyspark.sql.types import StructField

from snowflake.snowpark_checkpoints_collector.collection_common import (
    COLUMN_COUNT_KEY,
    COLUMN_METADATA_KEY,
    COLUMN_ROWS_NOT_NULL_COUNT_KEY,
    COLUMN_ROWS_NULL_COUNT_KEY,
    FIELD_METADATA_KEY,
    FIELDS_KEY,
    NAME_KEY,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model.column_collector_base import (
    ColumnCollectorBase,
)


class StructColumnCollector(ColumnCollectorBase):

    """Class for collect a struct type column.

    Attributes:
        name (str): the name of the column.
        type (str): the type of the column.
        struct_field (pyspark.sql.types.StructField): the struct field of the column type.
        values (pandas.Series): the column values as Pandas.Series.

    """

    def __init__(
        self, clm_name: str, struct_field: StructField, clm_values: Series
    ) -> None:
        """Init StructColumnCollector.

        Args:
            clm_name (str): the name of the column.
            struct_field (pyspark.sql.types.StructField): the struct field of the column type.
            clm_values (pandas.Series): the column values as Pandas.Series.

        """
        super().__init__(clm_name, struct_field, clm_values)

    def get_custom_data(self) -> dict[str, any]:
        metadata = self._compute_struct_metadata()

        custom_data_dict = {
            COLUMN_METADATA_KEY: metadata,
        }

        return custom_data_dict

    def _compute_struct_metadata(self) -> list[dict[str, any]]:
        field_metadata_collection = []
        struct_field_json = self.struct_field.dataType.jsonValue()
        for field in struct_field_json[FIELDS_KEY]:
            del field[FIELD_METADATA_KEY]
            clm_name = field[NAME_KEY]
            rows_count_dict = self._compute_rows_count_by_column(clm_name)
            struct_field_custom_data = dict(field | rows_count_dict)
            field_metadata_collection.append(struct_field_custom_data)

        return field_metadata_collection

    def _compute_rows_count_by_column(self, clm_name: str) -> dict[str, int]:
        rows_count = 0
        rows_not_null_count = 0
        rows_null_count = 0
        for row in self.values:
            row_clm_value = row[clm_name]
            rows_count += 1
            if row_clm_value is None:
                rows_null_count += 1
            else:
                rows_not_null_count += 1

        rows_count_dict = {
            COLUMN_COUNT_KEY: rows_count,
            COLUMN_ROWS_NOT_NULL_COUNT_KEY: rows_not_null_count,
            COLUMN_ROWS_NULL_COUNT_KEY: rows_null_count,
        }

        return rows_count_dict

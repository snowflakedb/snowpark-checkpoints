#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark_checkpoints_collector.collection_common import (
    BOOLEAN_COLUMN_TYPE,
    BYTE_COLUMN_TYPE,
    DATE_COLUMN_TYPE,
    DAYTIMEINTERVAL_COLUMN_TYPE,
    DOUBLE_COLUMN_TYPE,
    FLOAT_COLUMN_TYPE,
    INTEGER_COLUMN_TYPE,
    LONG_COLUMN_TYPE,
    SHORT_COLUMN_TYPE,
    STRING_COLUMN_TYPE,
    TIMESTAMP_COLUMN_TYPE,
    TIMESTAMP_NTZ_COLUMN_TYPE,
)
from snowflake.snowpark_checkpoints_collector.column_collection.model import (
    BooleanColumnCollector,
    DateColumnCollector,
    DayTimeIntervalColumnCollector,
    EmptyColumnCollector,
    NumericColumnCollector,
    StringColumnCollector,
    TimestampColumnCollector,
    TimestampNTZColumnCollector,
)


def collector_register(cls):
    """Decorate a class.

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
    """Decorate a method.

    Args:
        args: The decorator arguments.

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
    def collect_column(self, clm_name, clm_type, values) -> dict[str, any]:
        if clm_type not in self._collectors:
            return {}

        func_name = self._collectors[clm_type]
        func = getattr(self, func_name)
        data = func(clm_name, clm_type, values)
        return data

    @column_register(BOOLEAN_COLUMN_TYPE)
    def _collect_boolean_type_custom_data(
        self, clm_name, clm_type, values
    ) -> dict[str, any]:
        column_collector = BooleanColumnCollector(clm_name, clm_type, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(DATE_COLUMN_TYPE)
    def _collect_date_type_custom_data(
        self, clm_name, clm_type, values
    ) -> dict[str, any]:
        column_collector = DateColumnCollector(clm_name, clm_type, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(DAYTIMEINTERVAL_COLUMN_TYPE)
    def _collect_dayTimeInterval_type_custom_data(
        self, clm_name, clm_type, values
    ) -> dict[str, any]:
        column_collector = DayTimeIntervalColumnCollector(clm_name, clm_type, values)
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
        self, clm_name, clm_type, values
    ) -> dict[str, any]:
        column_collector = NumericColumnCollector(clm_name, clm_type, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(STRING_COLUMN_TYPE)
    def _collect_string_type_custom_data(
        self, clm_name, clm_type, values
    ) -> dict[str, any]:
        column_collector = StringColumnCollector(clm_name, clm_type, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(TIMESTAMP_COLUMN_TYPE)
    def _collect_timestamp_type_custom_data(
        self, clm_name, clm_type, values
    ) -> dict[str, any]:
        column_collector = TimestampColumnCollector(clm_name, clm_type, values)
        collected_data = column_collector.get_data()
        return collected_data

    @column_register(TIMESTAMP_NTZ_COLUMN_TYPE)
    def _collect_timestampntz_type_custom_data(
        self, clm_name, clm_type, values
    ) -> dict[str, any]:
        column_collector = TimestampNTZColumnCollector(clm_name, clm_type, values)
        collected_data = column_collector.get_data()
        return collected_data

    def collect_empty_custom_data(self, clm_name, clm_type, values) -> dict[str, any]:
        column_collector = EmptyColumnCollector(clm_name, clm_type, values)
        collected_data = column_collector.get_data()
        return collected_data

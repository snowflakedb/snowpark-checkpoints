#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pandas import DataFrame as PandasDataFrame
from pandera import Check, Column

from snowflake.snowpark_checkpoints_collector.collection_common import (
    BETWEEN_CHECK_ERROR_MESSAGE_FORMAT,
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


def collector_register(cls):
    """Decorate a class with the checks mechanism.

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
    """Decorate a method to register it in the checks mechanism based on column type.

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
class PanderaColumnChecksManager:

    """Manage class for Pandera column checks based on type."""

    def add_checks_column(
        self,
        clm_name: str,
        clm_type: str,
        pandas_df: PandasDataFrame,
        pandera_column: Column,
    ) -> None:
        """Add checks to Pandera column based on the column type.

        Args:
            clm_name (str): the name of the column.
            clm_type (str): the type of the column.
            pandas_df (pandas.DataFrame): the DataFrame.
            pandera_column (pandera.Column): the Pandera column.

        """
        if clm_type not in self._collectors:
            return

        func_name = self._collectors[clm_type]
        func = getattr(self, func_name)
        func(clm_name, pandas_df, pandera_column)

    @column_register(BOOLEAN_COLUMN_TYPE)
    def _add_boolean_type_checks(
        self, clm_name: str, pandas_df: PandasDataFrame, pandera_column: Column
    ) -> None:
        pandera_column.checks.extend([Check.isin([True, False])])

    @column_register(DATE_COLUMN_TYPE)
    def _add_date_type_checks(
        self, clm_name: str, pandas_df: PandasDataFrame, pandera_column: Column
    ) -> None:
        column_values = pandas_df[clm_name]
        min_value = str(column_values.min())
        max_value = str(column_values.max())
        pandera_column.checks.append(
            Check.between(
                min_value=min_value,
                max_value=max_value,
                include_max=True,
                include_min=True,
                title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value),
            )
        )

    @column_register(DAYTIMEINTERVAL_COLUMN_TYPE)
    def _add_daytimeinterval_type_checks(
        self, clm_name: str, pandas_df: PandasDataFrame, pandera_column: Column
    ) -> None:
        column_values = pandas_df[clm_name]
        min_value = str(column_values.min())
        max_value = str(column_values.max())
        pandera_column.checks.append(
            Check.between(
                min_value=min_value,
                max_value=max_value,
                include_max=True,
                include_min=True,
                title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value),
            )
        )

    @column_register(
        BYTE_COLUMN_TYPE,
        SHORT_COLUMN_TYPE,
        INTEGER_COLUMN_TYPE,
        LONG_COLUMN_TYPE,
        FLOAT_COLUMN_TYPE,
        DOUBLE_COLUMN_TYPE,
    )
    def _add_numeric_type_checks(
        self, clm_name: str, pandas_df: PandasDataFrame, pandera_column: Column
    ) -> None:
        column_values = pandas_df[clm_name]
        min_value = column_values.min().item()
        max_value = column_values.max().item()
        pandera_column.checks.append(
            Check.between(
                min_value=min_value,
                max_value=max_value,
                include_max=True,
                include_min=True,
                title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value),
            )
        )

    @column_register(STRING_COLUMN_TYPE)
    def _add_string_type_checks(
        self, clm_name: str, pandas_df: PandasDataFrame, pandera_column: Column
    ) -> None:
        pass

    @column_register(TIMESTAMP_COLUMN_TYPE)
    def _add_timestamp_type_checks(
        self, clm_name: str, pandas_df: PandasDataFrame, pandera_column: Column
    ) -> None:
        column_values = pandas_df[clm_name]
        min_value = str(column_values.min())
        max_value = str(column_values.max())
        pandera_column.checks.append(
            Check.between(
                min_value=min_value,
                max_value=max_value,
                include_max=True,
                include_min=True,
                title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value),
            )
        )

    @column_register(TIMESTAMP_NTZ_COLUMN_TYPE)
    def _add_timestamp_ntz_type_checks(
        self, clm_name: str, pandas_df: PandasDataFrame, pandera_column: Column
    ) -> None:
        column_values = pandas_df[clm_name]
        min_value = str(column_values.min())
        max_value = str(column_values.max())
        pandera_column.checks.append(
            Check.between(
                min_value=min_value,
                max_value=max_value,
                include_max=True,
                include_min=True,
                title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value),
            )
        )

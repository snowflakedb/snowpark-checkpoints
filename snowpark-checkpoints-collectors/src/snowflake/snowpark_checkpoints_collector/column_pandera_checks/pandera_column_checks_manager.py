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
import pandas as pd

from pandera import Check, Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col as spark_col
from pyspark.sql.functions import length as spark_length
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min

from snowflake.snowpark_checkpoints_collector.collection_common import (
    BETWEEN_CHECK_ERROR_MESSAGE_FORMAT,
    BOOLEAN_COLUMN_TYPE,
    BYTE_COLUMN_TYPE,
    COLUMN_MAX_KEY,
    COLUMN_MIN_KEY,
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
        pyspark_df: SparkDataFrame,
        pandera_column: Column,
    ) -> None:
        """Add checks to Pandera column based on the column type.

        Args:
            clm_name (str): the name of the column.
            clm_type (str): the type of the column.
            pyspark_df (pyspark.sql.DataFrame): the DataFrame.
            pandera_column (pandera.Column): the Pandera column.

        """
        if clm_type not in self._collectors:
            return

        func_name = self._collectors[clm_type]
        func = getattr(self, func_name)
        func(clm_name, pyspark_df, pandera_column)

    @column_register(BOOLEAN_COLUMN_TYPE)
    def _add_boolean_type_checks(
        self, clm_name: str, pyspark_df: SparkDataFrame, pandera_column: Column
    ) -> None:
        pandera_column.checks.extend([Check.isin([True, False])])

    @column_register(DAYTIMEINTERVAL_COLUMN_TYPE)
    def _add_daytimeinterval_type_checks(
        self, clm_name: str, pyspark_df: SparkDataFrame, pandera_column: Column
    ) -> None:
        select_result = pyspark_df.select(
            spark_min(spark_col(clm_name)).alias(COLUMN_MIN_KEY),
            spark_max(spark_col(clm_name)).alias(COLUMN_MAX_KEY),
        ).collect()[0]

        min_value = pd.to_timedelta(select_result[COLUMN_MIN_KEY])
        max_value = pd.to_timedelta(select_result[COLUMN_MAX_KEY])

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
        self, clm_name: str, pyspark_df: SparkDataFrame, pandera_column: Column
    ) -> None:
        select_result = pyspark_df.select(
            spark_min(spark_col(clm_name)).alias(COLUMN_MIN_KEY),
            spark_max(spark_col(clm_name)).alias(COLUMN_MAX_KEY),
        ).collect()[0]

        min_value = select_result[COLUMN_MIN_KEY]
        max_value = select_result[COLUMN_MAX_KEY]

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
        self, clm_name: str, pyspark_df: SparkDataFrame, pandera_column: Column
    ) -> None:
        select_result = pyspark_df.select(
            spark_min(spark_length(spark_col(clm_name))).alias(COLUMN_MIN_KEY),
            spark_max(spark_length(spark_col(clm_name))).alias(COLUMN_MAX_KEY),
        ).collect()[0]

        min_length = select_result[COLUMN_MIN_KEY]
        max_length = select_result[COLUMN_MAX_KEY]

        pandera_column.checks.append(Check.str_length(min_length, max_length))

    @column_register(TIMESTAMP_COLUMN_TYPE)
    def _add_timestamp_type_checks(
        self, clm_name: str, pyspark_df: SparkDataFrame, pandera_column: Column
    ) -> None:
        select_result = pyspark_df.select(
            spark_min(spark_col(clm_name)).alias(COLUMN_MIN_KEY),
            spark_max(spark_col(clm_name)).alias(COLUMN_MAX_KEY),
        ).collect()[0]

        min_value = pd.Timestamp(select_result[COLUMN_MIN_KEY])
        max_value = pd.Timestamp(select_result[COLUMN_MAX_KEY])

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
        self, clm_name: str, pyspark_df: SparkDataFrame, pandera_column: Column
    ) -> None:
        select_result = pyspark_df.select(
            spark_min(spark_col(clm_name)).alias(COLUMN_MIN_KEY),
            spark_max(spark_col(clm_name)).alias(COLUMN_MAX_KEY),
        ).collect()[0]

        min_value = pd.Timestamp(select_result[COLUMN_MIN_KEY])
        max_value = pd.Timestamp(select_result[COLUMN_MAX_KEY])

        pandera_column.checks.append(
            Check.between(
                min_value=min_value,
                max_value=max_value,
                include_max=True,
                include_min=True,
                title=BETWEEN_CHECK_ERROR_MESSAGE_FORMAT.format(min_value, max_value),
            )
        )

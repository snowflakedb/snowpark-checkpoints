#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
import datetime
import json

from typing import Optional

import dateutil
import pandera as pa

from hypothesis.strategies import DrawFn, SearchStrategy, composite

from snowflake.hypothesis_snowpark.checks import (
    dates_in_range,  # noqa: F401 Import required to register the custom checks
)
from snowflake.hypothesis_snowpark.constants import (
    CUSTOM_DATA_COLUMNS_KEY,
    CUSTOM_DATA_FORMAT_KEY,
    CUSTOM_DATA_KEY,
    CUSTOM_DATA_NAME_KEY,
    PANDERA_IN_RANGE_CHECK,
    PANDERA_INCLUDE_MAX_KEY,
    PANDERA_INCLUDE_MIN_KEY,
    PANDERA_MAX_VALUE_KEY,
    PANDERA_MIN_VALUE_KEY,
    PANDERA_SCHEMA_KEY,
)
from snowflake.hypothesis_snowpark.strategies_utils import (
    apply_custom_null_values,
    generate_snowpark_dataframe,
    load_json_schema,
)
from snowflake.snowpark import DataFrame, Session


def dataframe_strategy(
    json_schema: str, session: Session, size: Optional[int] = None
) -> SearchStrategy[DataFrame]:
    """Create a Hypothesis strategy for generating Snowpark DataFrames based on a Pandera JSON schema.

    Args:
        json_schema: The path to the JSON schema file.
        session: The Snowpark session to use for creating the DataFrames.
        size: The number of rows to generate. If not specified, the strategy will generate an arbitrary number of rows.

    Examples:
        >>> from hypothesis import given
        >>> from snowflake.hypothesis_snowpark import dataframe_strategy
        >>> from snowflake.snowpark import DataFrame, Session
        >>> @given(df=dataframe_strategy(json_schema="schema.json", session=Session.builder.getOrCreate(), size=10))
        >>> def test_my_function(df: DataFrame):
        >>>     ...

    Returns:
        A Hypothesis strategy that generates Snowpark DataFrames.

    """
    if not json_schema:
        raise ValueError("JSON schema cannot be None.")

    if not session:
        raise ValueError("Session cannot be None.")

    json_schema_dict = load_json_schema(json_schema)
    pandera_schema = json_schema_dict.get(PANDERA_SCHEMA_KEY)
    custom_data = json_schema_dict.get(CUSTOM_DATA_KEY)

    if not (pandera_schema and custom_data):
        raise ValueError(
            f"Invalid JSON schema. The JSON schema must contain '{PANDERA_SCHEMA_KEY}' and '{CUSTOM_DATA_KEY}' keys."
        )

    df_schema = pa.DataFrameSchema.from_json(json.dumps(pandera_schema))
    df_schema = _process_dataframe_schema(df_schema, custom_data)

    @composite
    def _dataframe_strategy(draw: DrawFn) -> DataFrame:
        pandas_strategy = df_schema.strategy(size=size)
        pandas_df = draw(pandas_strategy)
        processed_pandas_df = apply_custom_null_values(pandas_df, custom_data)
        snowpark_df = generate_snowpark_dataframe(
            processed_pandas_df, session, df_schema, custom_data
        )
        return snowpark_df

    return _dataframe_strategy()


def _process_dataframe_schema(
    df_schema: pa.DataFrameSchema, custom_data: dict
) -> pa.DataFrameSchema:
    df_schema_copy = copy.copy(df_schema)

    for column_name, column_obj in df_schema_copy.columns.items():
        if type(column_obj.dtype) is pa.engines.pandas_engine.Date:
            # Data generation for date type is currently unsupported by Pandera. As a workaround, we can change the data
            # type to pa.DateTime to avoid an exception and manually generate the dates.
            column_obj.dtype = pa.DateTime

            in_range_check = next(
                (
                    check
                    for check in column_obj.checks
                    if check.name == PANDERA_IN_RANGE_CHECK
                ),
                None,
            )

            if in_range_check is None:
                # Generate the values as DateTime and let Snowpark handle the conversion to Date.
                continue

            min_value = in_range_check.statistics.get(PANDERA_MIN_VALUE_KEY)
            max_value = in_range_check.statistics.get(PANDERA_MAX_VALUE_KEY)
            include_min = in_range_check.statistics.get(PANDERA_INCLUDE_MIN_KEY, True)
            include_max = in_range_check.statistics.get(PANDERA_INCLUDE_MAX_KEY, True)
            date_format = next(
                (
                    column.get(CUSTOM_DATA_FORMAT_KEY)
                    for column in custom_data.get(CUSTOM_DATA_COLUMNS_KEY, [])
                    if column.get(CUSTOM_DATA_NAME_KEY) == column_name
                ),
                None,
            )

            if date_format is not None:
                min_value_obj = datetime.datetime.strptime(
                    min_value, date_format
                ).date()
                max_value_obj = datetime.datetime.strptime(
                    max_value, date_format
                ).date()
            else:
                min_value_obj = dateutil.parser.parse(min_value).date()
                max_value_obj = dateutil.parser.parse(max_value).date()

            # Replace the previous checks with the new date range check.
            column_obj.checks = [
                pa.Check.dates_in_range(
                    min_value=min_value_obj,
                    max_value=max_value_obj,
                    include_min=include_min,
                    include_max=include_max,
                )
            ]

    return df_schema_copy

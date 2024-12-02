#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json

from typing import Final, Optional

import pandera as pa

from hypothesis.strategies import DrawFn, SearchStrategy, composite

from snowflake.hypothesis_snowpark.strategies_utils import (
    apply_null_values,
    generate_snowpark_dataframe,
    load_json_schema,
)
from snowflake.snowpark import DataFrame, Session


PANDERA_SCHEMA_KEY: Final[str] = "pandera_schema"
CUSTOM_DATA_KEY: Final[str] = "custom_data"


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

    if not pandera_schema or not custom_data:
        raise ValueError(
            f"Invalid JSON schema. The JSON schema must contain '{PANDERA_SCHEMA_KEY}' and '{CUSTOM_DATA_KEY}' keys."
        )

    df_schema = pa.DataFrameSchema.from_json(json.dumps(pandera_schema))

    @composite
    def _dataframe_strategy(draw: DrawFn) -> DataFrame:
        pandas_strategy = df_schema.strategy(size=size)
        pandas_df = draw(pandas_strategy)
        processed_pandas_df = apply_null_values(pandas_df, custom_data)
        snowpark_df = generate_snowpark_dataframe(
            processed_pandas_df, session, df_schema, custom_data
        )
        return snowpark_df

    return _dataframe_strategy()

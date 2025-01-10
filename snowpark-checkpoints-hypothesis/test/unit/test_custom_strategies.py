#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime as dt

from decimal import Decimal
from typing import Final

import hypothesis.strategies as st
import pandas as pd
import pytest

from hypothesis import given, settings, HealthCheck

from snowflake.hypothesis_snowpark.constants import (
    CUSTOM_DATA_MAX_SIZE_KEY,
    CUSTOM_DATA_MIN_SIZE_KEY,
    CUSTOM_DATA_NAME_KEY,
    CUSTOM_DATA_TYPE_KEY,
    CUSTOM_DATA_VALUE_TYPE_KEY,
    PYSPARK_ARRAY_TYPE,
    PYSPARK_BINARY_TYPE,
    PYSPARK_INTEGER_TYPE,
    PYSPARK_STRING_TYPE,
)
from snowflake.hypothesis_snowpark.custom_strategies import (
    array_strategy,
    binary_strategy,
    boolean_strategy,
    byte_strategy,
    date_strategy,
    decimal_strategy,
    float_strategy,
    integer_strategy,
    long_strategy,
    short_strategy,
    string_strategy,
    update_pandas_df_strategy,
)


MAX_EXAMPLES: Final[int] = 5


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_array_strategy(data: st.DataObject):
    min_size = 1
    max_size = 5
    result = data.draw(
        array_strategy(dtype=PYSPARK_INTEGER_TYPE, min_size=min_size, max_size=max_size)
    )
    assert isinstance(result, list)
    assert min_size <= len(result) <= max_size
    assert all(isinstance(value, int) for value in result)


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES, suppress_health_check=list(HealthCheck))
def test_array_strategy_default_values(data: st.DataObject):
    result = data.draw(array_strategy(dtype=PYSPARK_STRING_TYPE))
    assert isinstance(result, list)
    assert len(result) >= 0
    assert all(isinstance(value, str) for value in result)


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_array_strategy_unsupported_dtype(data: st.DataObject):
    dtype = "unsupported_dtype"
    with pytest.raises(
        ValueError, match=f"Not implemented SearchStrategy for arrays of type '{dtype}'"
    ):
        data.draw(array_strategy(dtype=dtype))


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_binary_strategy(data: st.DataObject):
    min_size = 1
    max_size = 5
    result = data.draw(binary_strategy(min_size=min_size, max_size=max_size))
    assert isinstance(result, bytes)
    assert min_size <= len(result) <= max_size


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_binary_strategy_default_values(data: st.DataObject):
    result = data.draw(binary_strategy())
    assert isinstance(result, bytes)
    assert len(result) >= 0


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_boolean_strategy(data: st.DataObject):
    result = data.draw(boolean_strategy())
    assert isinstance(result, bool)


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_byte_strategy(data: st.DataObject):
    result = data.draw(byte_strategy())
    assert isinstance(result, int)
    assert -128 <= result <= 127


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_date_strategy(data: st.DataObject):
    min_date = dt.date(2021, 1, 1)
    max_date = dt.date(2021, 12, 31)
    result = data.draw(date_strategy(min_value=min_date, max_value=max_date))
    assert isinstance(result, dt.date)
    assert min_date <= result <= max_date


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_date_strategy_default_values(data: st.DataObject):
    result = data.draw(date_strategy())
    assert isinstance(result, dt.date)
    assert dt.date.min <= result <= dt.date.max


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_decimal_strategy(data: st.DataObject):
    min_value = 0
    max_value = 10
    result = data.draw(decimal_strategy(min_value=min_value, max_value=max_value))
    assert isinstance(result, Decimal)
    assert min_value <= result <= max_value


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_decimal_strategy_default_values(data: st.DataObject):
    result = data.draw(decimal_strategy())
    assert isinstance(result, Decimal)


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_float_strategy(data: st.DataObject):
    min_value = 0
    max_value = 10
    result = data.draw(float_strategy(min_value=min_value, max_value=max_value))
    assert isinstance(result, float)
    assert min_value <= result <= max_value


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_float_strategy_default_values(data: st.DataObject):
    result = data.draw(float_strategy())
    assert isinstance(result, float)


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_integer_strategy(data: st.DataObject):
    min_value = 0
    max_value = 10
    result = data.draw(integer_strategy(min_value=min_value, max_value=max_value))
    assert isinstance(result, int)
    assert min_value <= result <= max_value


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_integer_strategy_default_values(data: st.DataObject):
    result = data.draw(integer_strategy())
    assert isinstance(result, int)


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_long_strategy(data: st.DataObject):
    result = data.draw(long_strategy())
    assert isinstance(result, int)
    assert -(2**63) <= result <= (2**63) - 1


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_short_strategy(data: st.DataObject):
    result = data.draw(short_strategy())
    assert isinstance(result, int)
    assert -(2**15) <= result <= 2**15 - 1


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_string_strategy(data: st.DataObject):
    min_size = 1
    max_size = 5
    result = data.draw(string_strategy(min_size=min_size, max_size=max_size))
    assert isinstance(result, str)
    assert min_size <= len(result) <= max_size


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_string_strategy_default_values(data: st.DataObject):
    result = data.draw(string_strategy())
    assert isinstance(result, str)
    assert len(result) >= 0


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_update_pandas_df_strategy_empty_columns(data: st.DataObject):
    df = pd.DataFrame({"col1": [1, 2, 3]})
    result_df = data.draw(update_pandas_df_strategy(df, []))
    pd.testing.assert_frame_equal(df, result_df)


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_update_pandas_df_strategy_array_type(data: st.DataObject):
    col_name = "col1"
    min_size = 1
    max_size = 3

    df = pd.DataFrame({col_name: ["a", "b", "c"]})
    columns = [
        {
            CUSTOM_DATA_TYPE_KEY: PYSPARK_ARRAY_TYPE,
            CUSTOM_DATA_VALUE_TYPE_KEY: PYSPARK_INTEGER_TYPE,
            CUSTOM_DATA_MIN_SIZE_KEY: min_size,
            CUSTOM_DATA_MAX_SIZE_KEY: max_size,
            CUSTOM_DATA_NAME_KEY: col_name,
        }
    ]
    result_df = data.draw(update_pandas_df_strategy(df, columns))

    assert result_df[col_name].apply(lambda x: isinstance(x, list)).all()
    assert result_df[col_name].apply(lambda x: min_size <= len(x) <= max_size).all()
    assert result_df[col_name].apply(lambda x: all(isinstance(i, int) for i in x)).all()


@given(data=st.data())
@settings(max_examples=MAX_EXAMPLES)
def test_update_pandas_df_strategy_binary_type(data: st.DataObject):
    col_name = "col1"
    min_size = 1
    max_size = 3

    df = pd.DataFrame({col_name: ["a", "b", "c"]})
    columns = [
        {
            CUSTOM_DATA_TYPE_KEY: PYSPARK_BINARY_TYPE,
            CUSTOM_DATA_MIN_SIZE_KEY: min_size,
            CUSTOM_DATA_MAX_SIZE_KEY: max_size,
            CUSTOM_DATA_NAME_KEY: col_name,
        }
    ]
    result_df = data.draw(update_pandas_df_strategy(df, columns))

    assert result_df[col_name].apply(lambda x: isinstance(x, bytes)).all()
    assert result_df[col_name].apply(lambda x: min_size <= len(x) <= max_size).all()

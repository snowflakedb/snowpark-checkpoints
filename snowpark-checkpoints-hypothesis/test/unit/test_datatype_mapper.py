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

from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pandera as pa
import pytest

from snowflake.hypothesis_snowpark.constants import PYSPARK_TO_SNOWPARK_SUPPORTED_TYPES
from snowflake.hypothesis_snowpark.datatype_mapper import (
    pandera_dtype_to_snowpark_dtype,
    pyspark_dtype_to_snowpark_dtype,
)
from snowflake.snowpark.types import (
    BooleanType,
    ByteType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampTimeZone,
    TimestampType,
)
from snowflake.snowpark.types import (
    DataType as SnowparkDataType,
)


def test_pandera_dtype_to_snowpark_dtype_unsupported():
    with pytest.raises(
        TypeError,
        match="Unsupported Pandera data type: <class 'pandera.dtypes.Category'>",
    ):
        pandera_dtype_to_snowpark_dtype(pa.Category)


def test_pandera_dtype_to_snowpark_dtype_byte():
    column_type_mapping = [
        (pa.Column(pa.Int8), ByteType()),
        (pa.Column(pa.INT8), ByteType()),
        (pa.Column("int8"), ByteType()),
        (pa.Column(np.int8), ByteType()),
        (pa.Column(pd.Int8Dtype), ByteType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_short():
    column_type_mapping = [
        (pa.Column(pa.Int16), ShortType()),
        (pa.Column(pa.INT16), ShortType()),
        (pa.Column("int16"), ShortType()),
        (pa.Column(np.int16), ShortType()),
        (pa.Column(pd.Int16Dtype), ShortType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_integer():
    column_type_mapping = [
        (pa.Column(pa.Int32), IntegerType()),
        (pa.Column(pa.INT32), IntegerType()),
        (pa.Column("int32"), IntegerType()),
        (pa.Column(np.int32), IntegerType()),
        (pa.Column(pd.Int32Dtype), IntegerType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_long():
    column_type_mapping = [
        (pa.Column(pa.Int64), LongType()),
        (pa.Column(pa.INT64), LongType()),
        (pa.Column(pa.Int), LongType()),
        (pa.Column(int), LongType()),
        (pa.Column("int"), LongType()),
        (pa.Column("int64"), LongType()),
        (pa.Column(np.int64), LongType()),
        (pa.Column(np.int_), LongType()),
        (pa.Column(pd.Int64Dtype), LongType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_float():
    column_type_mapping = [
        (pa.Column(pa.Float32), FloatType()),
        (pa.Column("float32"), FloatType()),
        (pa.Column(np.float32), FloatType()),
        (pa.Column(pd.Float32Dtype), FloatType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_double():
    column_type_mapping = [
        (pa.Column(pa.Float64), DoubleType()),
        (pa.Column(float), DoubleType()),
        (pa.Column("float"), DoubleType()),
        (pa.Column("float64"), DoubleType()),
        (pa.Column(np.float64), DoubleType()),
        (pa.Column(pd.Float64Dtype), DoubleType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_string():
    column_type_mapping = [
        (pa.Column(pa.String), StringType()),
        (pa.Column(pa.STRING), StringType()),
        (pa.Column(str), StringType()),
        (pa.Column("str"), StringType()),
        (pa.Column("string"), StringType()),
        (pa.Column(np.str_), StringType()),
        (pa.Column(pd.StringDtype), StringType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_boolean():
    column_type_mapping = [
        (pa.Column(pa.Bool), BooleanType()),
        (pa.Column(pa.BOOL), BooleanType()),
        (pa.Column(bool), BooleanType()),
        (pa.Column("bool"), BooleanType()),
        (pa.Column("boolean"), BooleanType()),
        (pa.Column(np.bool_), BooleanType()),
        (pa.Column(pd.BooleanDtype), BooleanType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_timestamp():
    column_type_mapping = [
        (
            pa.Column(pd.DatetimeTZDtype(tz=ZoneInfo("Europe/Paris"))),
            TimestampType(TimestampTimeZone.TZ),
        ),
        (
            pa.Column("datetime64[ns, Europe/Paris]"),
            TimestampType(TimestampTimeZone.TZ),
        ),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_timestamp_ntz():
    column_type_mapping = [
        (pa.Column(pa.Timestamp), TimestampType(TimestampTimeZone.NTZ)),
        (pa.Column(pa.DateTime), TimestampType(TimestampTimeZone.NTZ)),
        (pa.Column(datetime), TimestampType(TimestampTimeZone.NTZ)),
        (pa.Column("datetime"), TimestampType(TimestampTimeZone.NTZ)),
        (pa.Column("datetime64[ns]"), TimestampType(TimestampTimeZone.NTZ)),
        (pa.Column(np.datetime64), TimestampType(TimestampTimeZone.NTZ)),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pandera_dtype_to_snowpark_dtype_timestamp_date():
    column_type_mapping = [
        (pa.Column(pa.Date), DateType()),
        (pa.Column(date), DateType()),
        (pa.Column("date"), DateType()),
    ]
    _validate_column_type_mapping(column_type_mapping)


def test_pyspark_dtype_to_snowpark_dtype_valid_types():
    for (
        pyspark_type,
        expected_snowpark_type,
    ) in PYSPARK_TO_SNOWPARK_SUPPORTED_TYPES.items():
        actual_snowpark_type = pyspark_dtype_to_snowpark_dtype(pyspark_type)
        assert isinstance(actual_snowpark_type, SnowparkDataType)
        assert actual_snowpark_type == expected_snowpark_type


def test_pyspark_dtype_to_snowpark_dtype_invalid_types():
    pyspark_type = "unknown_type"
    with pytest.raises(
        TypeError, match=f"Unsupported PySpark data type: {pyspark_type}"
    ):
        pyspark_dtype_to_snowpark_dtype(pyspark_type)


def _validate_column_type_mapping(
    column_type_mapping: list[tuple[pa.Column, SnowparkDataType]]
):
    for column, expected_snowpark_type in column_type_mapping:
        assert pandera_dtype_to_snowpark_dtype(column.dtype) == expected_snowpark_type

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

import numpy as np
import pandas as pd
import pandera as pa

from snowflake.hypothesis_snowpark.constants import (
    PYSPARK_TO_SNOWPARK_SUPPORTED_TYPES,
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


pandera_dtype_to_snowpark_dtype_dict = {
    np.dtypes.Int8DType: ByteType(),
    pd.Int8Dtype: ByteType(),
    np.dtypes.Int16DType: ShortType(),
    pd.Int16Dtype: ShortType(),
    np.dtypes.Int32DType: IntegerType(),
    pd.Int32Dtype: IntegerType(),
    np.dtypes.Int64DType: LongType(),
    pd.Int64Dtype: LongType(),
    np.dtypes.Float32DType: FloatType(),
    pd.Float32Dtype: FloatType(),
    np.dtypes.Float64DType: DoubleType(),
    pd.Float64Dtype: DoubleType(),
    np.dtypes.StrDType: StringType(),
    pd.StringDtype: StringType(),
    np.dtypes.BoolDType: BooleanType(),
    pd.BooleanDtype: BooleanType(),
    pd.DatetimeTZDtype: TimestampType(TimestampTimeZone.TZ),
    np.dtypes.DateTime64DType: TimestampType(TimestampTimeZone.NTZ),
    pa.engines.pandas_engine.Date: DateType(),
}


def pandera_dtype_to_snowpark_dtype(pandera_dtype: pa.DataType) -> SnowparkDataType:
    """Convert a Pandera data type to the equivalent Snowpark data type.

    Args:
        pandera_dtype: The Pandera data type to convert.

    Raises:
        TypeError: If the Pandera data type is not supported.

    Returns:
        The equivalent Snowpark data type.

    """
    snowpark_dtype = pandera_dtype_to_snowpark_dtype_dict.get(
        type(pandera_dtype)
    ) or pandera_dtype_to_snowpark_dtype_dict.get(type(pandera_dtype.type))

    if not snowpark_dtype:
        raise TypeError(f"Unsupported Pandera data type: {pandera_dtype}")
    return snowpark_dtype


def pyspark_dtype_to_snowpark_dtype(pyspark_dtype: str) -> SnowparkDataType:
    """Convert a PySpark data type to the equivalent Snowpark data type.

    Args:
        pyspark_dtype: The PySpark data type to convert.

    Raises:
        ValueError: If the PySpark data type is not supported.

    Returns:
        The equivalent Snowpark data type.

    """
    snowpark_type = PYSPARK_TO_SNOWPARK_SUPPORTED_TYPES.get(pyspark_dtype)
    if not snowpark_type:
        raise TypeError(f"Unsupported PySpark data type: {pyspark_dtype}")
    return snowpark_type

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

from snowflake.snowpark_checkpoints.utils.constants import (
    BINARY_TYPE,
    BOOLEAN_TYPE,
    BYTE_TYPE,
    DATE_TYPE,
    DECIMAL_TYPE,
    DOUBLE_TYPE,
    FLOAT_TYPE,
    INTEGER_TYPE,
    LONG_TYPE,
    SHORT_TYPE,
    STRING_TYPE,
    TIMESTAMP_NTZ_TYPE,
    TIMESTAMP_TYPE,
)


NumericTypes = [
    BYTE_TYPE,
    SHORT_TYPE,
    INTEGER_TYPE,
    LONG_TYPE,
    FLOAT_TYPE,
    DOUBLE_TYPE,
    DECIMAL_TYPE,
]

StringTypes = [STRING_TYPE]

BinaryTypes = [BINARY_TYPE]

BooleanTypes = [BOOLEAN_TYPE]

DateTypes = [DATE_TYPE, TIMESTAMP_TYPE, TIMESTAMP_NTZ_TYPE]

SupportedTypes = [
    BYTE_TYPE,
    SHORT_TYPE,
    INTEGER_TYPE,
    LONG_TYPE,
    FLOAT_TYPE,
    DOUBLE_TYPE,
    DECIMAL_TYPE,
    STRING_TYPE,
    BINARY_TYPE,
    BOOLEAN_TYPE,
    DATE_TYPE,
    TIMESTAMP_TYPE,
    TIMESTAMP_NTZ_TYPE,
]

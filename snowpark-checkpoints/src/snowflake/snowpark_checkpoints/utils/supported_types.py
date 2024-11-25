#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Literal


numeric_types = Literal[
    "byte", "short", "integer", "long", "float", "double", "decimal"
]

string_types = Literal["string"]

binary_types = Literal["binary"]

boolean_types = Literal["boolean"]

date_types = Literal["date", "timestamp", "timestamp_ntz"]

supported_types = Literal[
    "byte",
    "short",
    "integer",
    "long",
    "float",
    "double",
    "decimal",
    "string",
    "binary",
    "boolean",
    "date",
    "timestamp",
    "timestamp_ntz",
]

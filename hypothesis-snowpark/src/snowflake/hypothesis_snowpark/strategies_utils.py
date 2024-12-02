#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
import os

from typing import Final

import numpy as np
import pandas as pd
import pandera as pa

from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampTimeZone,
    TimestampType,
)


PYSPARK_TO_SNOWPARK_TYPES: Final[dict[str, DataType]] = {
    "array": ArrayType(),
    "binary": BinaryType(),
    "boolean": BooleanType(),
    "byte": ByteType(),
    "data": DataType(),
    "date": DateType(),
    "decimal": DecimalType(),
    "double": DoubleType(),
    "float": FloatType(),
    "integer": IntegerType(),
    "long": LongType(),
    "map": MapType(),
    "void": NullType(),
    "short": ShortType(),
    "string": StringType(),
    "timestamp": TimestampType(),
    "timestamp_ntz": TimestampType(TimestampTimeZone.NTZ),
}


def load_json_schema(json_schema: str) -> dict:
    """Load the JSON schema from a file.

    Args:
        json_schema: The path to the JSON file.

    Returns:
        The JSON schema as a dictionary.

    """
    if not os.path.isfile(json_schema):
        raise ValueError(f"Invalid JSON schema path: {json_schema}")

    try:
        with open(json_schema, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError) as e:
        raise ValueError(f"Error reading JSON schema file: {e}") from None


def pyspark_to_snowpark_type(pyspark_type: str) -> DataType:
    """Convert a PySpark data type to the equivalent Snowpark data type.

    Args:
        pyspark_type: The PySpark data type to convert.

    Raises:
        ValueError: If the PySpark data type is not supported.

    Returns:
        The equivalent Snowpark data type.

    """
    snowpark_type = PYSPARK_TO_SNOWPARK_TYPES.get(pyspark_type)
    if not snowpark_type:
        raise ValueError(f"Unsupported PySpark data type: {pyspark_type}")
    return snowpark_type


def generate_snowpark_dataframe(
    pandas_df: pd.DataFrame,
    session: Session,
    pandera_schema: pa.DataFrameSchema,
    custom_data: dict,
) -> DataFrame:
    """Generate a Snowpark DataFrame from a Pandas DataFrame.

    Args:
        pandas_df: The Pandas DataFrame to convert.
        session: The Snowpark session to use for creating the DataFrame.
        pandera_schema: The Pandera schema associated with the Pandas DataFrame.
        custom_data: The custom data associated with the Pandera schema.

    Returns:
        A Snowpark DataFrame.

    """

    def _generate_snowpark_schema() -> StructType:
        struct_fields = []
        for column_name, column_data in pandera_schema.columns.items():
            custom_data_column = next(
                (
                    column
                    for column in custom_data.get("columns", [])
                    if column.get("name") == column_name
                ),
                None,
            )

            if custom_data_column is None:
                raise ValueError(f"Column '{column_name}' is missing from custom_data")

            pyspark_type = custom_data_column.get("type")
            if pyspark_type is None:
                raise ValueError(
                    f"Type for column '{column_name}' is missing from custom_data"
                )

            snowpark_type = pyspark_to_snowpark_type(pyspark_type)
            nullable = column_data.nullable

            struct_field = StructField(
                column_identifier=column_name,
                datatype=snowpark_type,
                nullable=nullable,
            )
            struct_fields.append(struct_field)
        return StructType(struct_fields)

    # Snowpark ignores the schema argument if the data argument is a pandas dataframe. We need to convert the pandas
    # dataframe into a list of tuples to be able to specify a schema.
    data = list(pandas_df.itertuples(index=False, name=None))
    schema = _generate_snowpark_schema()
    snowpark_df = session.create_dataframe(data=data, schema=schema)
    return snowpark_df


def apply_null_values(pandas_df: pd.DataFrame, custom_data: dict) -> pd.DataFrame:
    """Apply null values to a Pandas DataFrame based on the custom data.

    Args:
        pandas_df: The Pandas DataFrame to apply null values to.
        custom_data: The custom data associated with the Pandera schema.

    Returns:
        A Pandas DataFrame with null values applied.

    """
    null_proportions = {
        col["name"]: col["rows_null_count"] / col["rows_count"]
        for col in custom_data.get("columns", [])
        if col.get("rows_count", 0) > 0
    }

    df = pandas_df.copy()
    total_rows = len(df)

    for column, target_null_proportion in null_proportions.items():
        if column not in df.columns:
            continue

        required_nulls = int(total_rows * target_null_proportion)
        current_nulls = df[column].isnull().sum()

        if current_nulls < required_nulls:
            additional_nulls = required_nulls - current_nulls
            non_null_positions = np.where(df[column].notnull())[0]
            selected_positions = np.random.choice(
                non_null_positions, size=additional_nulls, replace=False
            )
            df.iloc[selected_positions, df.columns.get_loc(column)] = None

    return df.replace({np.nan: None})

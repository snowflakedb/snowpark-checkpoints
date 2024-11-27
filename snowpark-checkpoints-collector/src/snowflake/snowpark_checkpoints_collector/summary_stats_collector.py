#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from decimal import Decimal

import numpy as np
import pandas
import pandera as pa

from pandera import Check
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import max, min


def collect_input_schema(df: SparkDataFrame):
    """Collect and return the input schema of a Spark DataFrame.

    Args:
        df (SparkDataFrame): The input Spark DataFrame.

    Returns:
        Optional[StructType]: The schema of the input DataFrame.

    """
    pass


def collect_output_schema(df: SparkDataFrame):
    """Collect and return the output schema of a Spark DataFrame.

    Args:
        df (SparkDataFrame): The output Spark DataFrame.

    Returns:
        Optional[StructType]: The schema of the output DataFrame.

    """
    pass


def convert_string_to_number(string_value):
    """Convert a string representation to either an integer or a float.

    This function attempts to convert a string to a number, preferring integer
    conversion for whole numbers and float conversion for decimal numbers.

    Args:
        string_value (str): The string to be converted to a number.

    Returns:
        Union[int, float]: The converted numeric value.

    Raises:
        ValueError: If the string cannot be converted to a number.

    """
    try:
        if "." in string_value:
            float_value = float(string_value)
            return float_value
        else:

            int_value = int(string_value)
            return int_value

    except ValueError as err:
        raise ValueError(f"Cannot convert {string_value} to a number") from err


def collect_df_schema(
    df: SparkDataFrame, checkpoint_name, sample=0.1, min_amnount_for_category=0.1
):
    """Collect and infer a Pandera schema for a Spark DataFrame.

    Args:
        df (SparkDataFrame): The input Spark DataFrame to analyze.
        checkpoint_name (str): The name of the checkpoint.
        sample (float, optional): Fraction of DataFrame to sample for schema inference.
            Defaults to 0.1.
        min_amnount_for_category (float, optional): Minimum proportion of rows required.
            Defaults to 0.1.

    """
    describe_df = df.describe().toPandas().set_index("summary")
    min_amnount_for_category = df.count() * min_amnount_for_category

    # use infer schema on a sample to set most values
    # this may be error prone

    sampled_df = df.sample(sample).toPandas()
    sampled_df.index = np.ones(sampled_df.count().iloc[0])

    schema = pa.infer_schema(sampled_df)

    for col in schema.columns:
        col_dtype = schema.columns[col].dtype.type
        schema.columns[col].checks = []

        if col_dtype == "empty":
            continue

        if col == "index":
            pass

        elif pandas.api.types.is_bool_dtype(col_dtype):
            schema.columns[col].checks.extend([Check.isin([True, False])])

        elif pandas.api.types.is_numeric_dtype(col_dtype):
            min_value = convert_string_to_number(describe_df.loc["min"][col])
            max_value = convert_string_to_number(describe_df.loc["max"][col])

            schema.columns[col].checks.append(
                Check.between(
                    min_value=min_value,
                    max_value=max_value,
                    include_min=True,
                    include_max=True,
                    title=f"Value should be between {describe_df.loc['min'][col]} and {describe_df.loc['max'][col]}",
                )
            )

        elif pandas.api.types.is_datetime64_any_dtype(col_dtype):
            append_min_and_max_to_schema(schema, df, col)

        elif pandas.api.types.is_object_dtype(col_dtype):
            # * decimal is and object so we need to check if it is a decimal, bytes, date or string

            if isinstance(sampled_df[col].iloc[0], Decimal):
                min_value = float(df.select(min(col)).head()[0])
                max_value = float(df.select(max(col)).head()[0])
                schema.columns[col].checks.append(
                    Check.between(
                        min_value=min_value,
                        max_value=max_value,
                        include_max=True,
                        include_min=True,
                        title=f"Value should be between {min_value} and {max_value}",
                    )
                )

            elif isinstance(sampled_df[col].iloc[0], bytes):
                append_min_and_max_to_schema(schema, df, col)

            elif isinstance(sampled_df[col].iloc[0], np.datetime64):
                append_min_and_max_to_schema(schema, df, col)

            elif isinstance(sampled_df[col].iloc[0], str):

                unique_values = df.groupBy(col).count().orderBy("count")

                if unique_values.head()[1] > min_amnount_for_category:
                    schema.columns[col].checks.append(
                        Check.isin(unique_values.select(col).toPandas()[col].to_list())
                    )

    f = open(f"snowpark-{checkpoint_name}-schema.json", "w")
    f.write(schema.to_json())
    f.close()


def append_min_and_max_to_schema(schema, df, col):
    """Append min and max value checks to a Pandera schema for a specific column.

    Args:
        schema (pa.DataFrameSchema): The Pandera schema to modify.
        df (SparkDataFrame): The Spark DataFrame to extract min and max values from.
        col (str): The column name to add min/max checks for.

    """
    min_value = df.select(min(col)).head()[0]
    max_value = df.select(max(col)).head()[0]
    schema.columns[col].checks.append(
        Check.between(
            min_value=min_value,
            max_value=max_value,
            include_max=True,
            include_min=True,
            title=f"Value should be between {min_value} and {max_value}",
        )
    )

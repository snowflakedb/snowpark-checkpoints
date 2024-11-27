#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json

from typing import Any

import pandera as pa

from pandera import DataFrameSchema

from .supported_types import boolean_types, numeric_types, supported_types


def add_numeric_checks(
    schema: DataFrameSchema, col: str, additional_check: dict[str, Any]
):
    """Add statistical checks for numeric columns in a Pandera DataFrame schema.

    This function adds a check to ensure the mean of a numeric column falls within
    a specified range based on the provided mean and standard deviation.

    Args:
        schema (DataFrameSchema): The Pandera DataFrame schema to modify.
        col (str): The name of the column to add checks to.
        additional_check (Dict[str, Any]): A dictionary containing check parameters.

    """
    mean = additional_check.get("mean", 0)
    std = additional_check.get("std", 0)

    def check_mean(series):
        series_mean = series.mean()
        return mean - std <= series_mean <= mean + std

    schema.columns[col].checks.append(pa.Check(check_mean, element_wise=True))


def add_boolean_checks(
    schema: DataFrameSchema, col: str, additional_check: dict[str, Any]
):
    """Add statistical checks for boolean columns in a Pandera DataFrame schema.

    This function adds checks to ensure the count of True and False values
    in a boolean column falls within specified ranges.

    Args:
        schema (DataFrameSchema): The Pandera DataFrame schema to modify.
        col (str): The name of the column to add checks to.
        additional_check (Dict[str, Any]): A dictionary containing check parameters.

    """
    count_of_true = additional_check.get("count_of_true", 0)
    count_of_false = additional_check.get("count_of_false", 0)
    std = additional_check.get("std", 0)

    schema.columns[col].checks.extend(
        [
            pa.Check(
                lambda series: count_of_true - std
                <= series.value_counts()[True]
                <= count_of_true + std
            ),
            pa.Check(
                lambda series: count_of_false - std
                <= series.value_counts()[False]
                <= count_of_false + std
            ),
        ]
    )


def generate_schema(checkpoint_name: str) -> DataFrameSchema:
    """Generate a Pandera DataFrame schema based on a JSON configuration file.

    This function reads a JSON schema configuration file and creates a Pandera
    DataFrame schema with optional additional checks for specific column types.

    Args:
        checkpoint_name (str): The name of the checkpoint used to identify
            the schema configuration file.

    Returns:
        DataFrameSchema: A Pandera DataFrame schema with optional additional checks.

    """
    additional_checks_schema = open(f"snowpark-{checkpoint_name}-schema.json")
    additional_checks_schema_json = json.load(additional_checks_schema)

    if "pandera_schema" in additional_checks_schema_json:
        schema_dict = additional_checks_schema_json.get("pandera_schema")
        schema = pa.DataFrameSchema.from_json(json.dumps(schema_dict))
    else:
        schema = pa.DataFrameSchema()

    if "additional_checks" in additional_checks_schema_json:
        for additional_check in additional_checks_schema_json.get("additional_checks"):
            type = additional_check.get("type", None)
            col = additional_check.get("col", None)

            if col is None or type is None:
                continue

            if type in supported_types:

                if type in numeric_types:
                    add_numeric_checks(schema, col, additional_check)
                elif type in boolean_types:
                    add_boolean_checks(schema, col, additional_check)

    return schema

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json

from typing import Any, Optional

import pandera as pa

from pandera import DataFrameSchema

from snowflake.snowpark_checkpoints.utils.constant import (
    CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT,
    DATAFRAME_CUSTOM_DATA_KEY,
    DATAFRAME_PANDERA_SCHEMA_KEY,
    DECIMAL_PRECISION_KEY,
    FALSE_COUNT_KEY,
    MARGIN_ERROR_KEY,
    MEAN_KEY,
    NAME_KEY,
    SKIP_ALL,
    TRUE_COUNT_KEY,
    TYPE_KEY,
)
from snowflake.snowpark_checkpoints.utils.supported_types import (
    BooleanTypes,
    NumericTypes,
    SupportedTypes,
)


def add_numeric_checks(
    schema: DataFrameSchema, col: str, additional_check: dict[str, Any]
):
    """Add numeric checks to a specified column in a DataFrameSchema.

    Args:
        schema (DataFrameSchema): The schema to which the checks will be added.
        col (str): The name of the column to which the checks will be applied.
        additional_check (dict[str, Any]): A dictionary containing additional checks.
            - MEAN_KEY (str): The key for the mean value to check against.
            - MARGIN_ERROR_KEY (str): The key for the margin of error for the mean check.
            - DECIMAL_PRECISION_KEY (str): The key for the maximum decimal precision allowed.

    Returns:
        None

    """
    mean = additional_check.get(MEAN_KEY, 0)
    std = additional_check.get(MARGIN_ERROR_KEY, 0)

    def check_mean(series):
        series_mean = series.mean()
        return mean - std <= series_mean <= mean + std

    schema.columns[col].checks.append(pa.Check(check_mean, element_wise=False))

    if DECIMAL_PRECISION_KEY in additional_check:
        schema.columns[col].checks.append(
            pa.Check(
                lambda series: series.apply(
                    lambda x: len(str(x).split(".")[1]) if "." in str(x) else 0
                )
                <= additional_check[DECIMAL_PRECISION_KEY]
            )
        )


def add_boolean_checks(
    schema: DataFrameSchema, col: str, additional_check: dict[str, Any]
):
    """Add boolean checks to a specified column in a DataFrameSchema.

    Args:
        schema (DataFrameSchema): The schema to which the checks will be added.
        col (str): The name of the column to which the checks will be applied.
        additional_check (dict[str, Any]): A dictionary containing additional check parameters.
            - TRUE_COUNT_KEY (int): Expected count of True values in the column.
            - FALSE_COUNT_KEY (int): Expected count of False values in the column.
            - MARGIN_ERROR_KEY (int): Margin of error allowed for the counts.

    Returns:
        None

    """
    count_of_true = additional_check.get(TRUE_COUNT_KEY, 0)
    count_of_false = additional_check.get(FALSE_COUNT_KEY, 0)
    std = additional_check.get(MARGIN_ERROR_KEY, 0)

    schema.columns[col].checks.extend(
        [
            pa.Check(
                lambda series: count_of_true - std
                <= series.value_counts().get(True, 0)
                <= count_of_true + std
            ),
            pa.Check(
                lambda series: count_of_false - std
                <= series.value_counts().get(False, 0)
                <= count_of_false + std
            ),
        ]
    )


def generate_schema(checkpoint_name: str) -> DataFrameSchema:
    """Generate a DataFrameSchema based on the checkpoint name provided.

    This function reads a JSON file corresponding to the checkpoint name,
    extracts schema information, and constructs a DataFrameSchema object.
    It also adds custom checks for numeric and boolean types if specified
    in the JSON file.

    Args:
        checkpoint_name (str): The name of the checkpoint used to locate
                               the JSON file containing schema information.

        DataFrameSchema: A schema object representing the structure and
                         constraints of the DataFrame.
                         constraints of the DataFrame.

    """
    with open(
        CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(checkpoint_name)
    ) as additional_checks_schema:

        additional_checks_schema_json = json.load(additional_checks_schema)

        if DATAFRAME_PANDERA_SCHEMA_KEY in additional_checks_schema_json:
            schema_dict = additional_checks_schema_json.get(
                DATAFRAME_PANDERA_SCHEMA_KEY
            )
            schema_dict_str = json.dumps(schema_dict)
            schema = pa.DataFrameSchema.from_json(schema_dict_str)
        else:
            schema = pa.DataFrameSchema()

        if DATAFRAME_CUSTOM_DATA_KEY in additional_checks_schema_json:
            for additional_check in additional_checks_schema_json.get(
                DATAFRAME_CUSTOM_DATA_KEY, []
            ):
                type = additional_check.get(TYPE_KEY, None)
                name = additional_check.get(NAME_KEY, None)

                if name is None or type is None:
                    continue

                if type in SupportedTypes:

                    if type in NumericTypes:
                        add_numeric_checks(schema, name, additional_check)

                    elif type in BooleanTypes:
                        add_boolean_checks(schema, name, additional_check)

        return schema


def skip_checks_on_schema(
    pandera_schema: DataFrameSchema,
    skip_checks: Optional[dict[str, list[str]]] = None,
):
    """Modify a Pandera DataFrameSchema to skip specified checks on certain columns.

    Args:
        pandera_schema (DataFrameSchema): The Pandera DataFrameSchema object to modify.
        skip_checks (Optional[dict[str, list[str]]]): A dictionary where keys are column names
                                                and values are lists of checks to skip for
                                                those columns. If the list is empty, all
                                                checks for the column will be removed.

    Returns:
        None

    """
    if skip_checks:
        for col, checks_to_skip in skip_checks.items():

            if col in pandera_schema.columns:

                if SKIP_ALL in checks_to_skip:
                    pandera_schema.columns[col].checks = {}

                else:
                    pandera_schema.columns[col].checks = [
                        check
                        for check in pandera_schema.columns[col].checks
                        if check.name not in checks_to_skip
                    ]

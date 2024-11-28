#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
from unittest.mock import patch, mock_open
from numpy import float64, int8
from pandera import DataFrameSchema
import pytest
from snowflake.snowpark_checkpoints.utils.constant import (
    BOOLEAN_TYPE,
    CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT,
    DATAFRAME_CUSTOM_DATA_KEY,
    FLOAT_TYPE,
    TYPE_KEY,
)
from snowflake.snowpark_checkpoints.utils.utils_checks import generate_schema
from pandera import Column, Check, DataFrameSchema
import pandas as pd
from snowflake.snowpark_checkpoints.utils.utils_checks import (
    add_numeric_checks,
    MEAN_KEY,
    MARGIN_ERROR_KEY,
    DECIMAL_PRECISION_KEY,
)
from snowflake.snowpark_checkpoints.utils.utils_checks import (
    skip_checks_on_schema,
    SKIP_ALL,
    add_boolean_checks,
    TRUE_COUNT_KEY,
    FALSE_COUNT_KEY,
    MARGIN_ERROR_KEY,
)


@pytest.fixture
def generate_json_schema_file():
    schema = DataFrameSchema(
        {
            "col1": Column(float64, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "col2": Column(bool, Check.isin([True, False])),
        }
    )

    schema_data = {
        "pandera_schema": json.loads(schema.to_json()),
        "custom_data": [
            {
                "type": FLOAT_TYPE,
                "name": "col1",
                "min": 4.50,
                "max": 5.55,
                "mean": 5.01,
                "decimal_precision": 2,
                "margin_error": 0.42,
            },
            {
                "type": BOOLEAN_TYPE,
                "name": "col2",
                "true_count": 2,
                "false_count": 1,
            },
        ],
    }

    checkpoint_name = "test_checkpoint"
    output_file = open(
        CHECKPOINT_JSON_OUTPUT_FILE_NAME_FORMAT.format(checkpoint_name), "w"
    )
    output_file.write(json.dumps(schema_data))
    output_file.close()


def test_skip_specific_check():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0), Check.less_than(10)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    skip_checks = {"col1": ["greater_than"]}
    skip_checks_on_schema(schema, skip_checks)

    assert len(schema.columns["col1"].checks) == 2
    assert schema.columns["col1"].checks[0].name == "greater_than"
    assert len(schema.columns["col2"].checks) == 1


def test_skip_checks_on_nonexistent_column():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0), Check.less_than(10)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    skip_checks = {"col3": [SKIP_ALL]}
    skip_checks_on_schema(schema, skip_checks)

    assert len(schema.columns["col1"].checks) == 2
    assert len(schema.columns["col2"].checks) == 1


def test_skip_no_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0), Check.less_than(10)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    skip_checks = {}
    skip_checks_on_schema(schema, skip_checks)

    assert len(schema.columns["col1"].checks) == 2
    assert len(schema.columns["col2"].checks) == 1


def test_add_boolean_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    additional_check = {TRUE_COUNT_KEY: 2, FALSE_COUNT_KEY: 1, MARGIN_ERROR_KEY: 0}

    add_boolean_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3  # initial check + 2 added checks

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [True, True, False]})

    # Validate the DataFrame against the schema
    schema.validate(df)


def test_add_boolean_checks_with_margin_error():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    additional_check = {TRUE_COUNT_KEY: 2, FALSE_COUNT_KEY: 1, MARGIN_ERROR_KEY: 1}

    add_boolean_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3  # initial check + 2 added checks

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [True, True, False, False]})

    # Validate the DataFrame against the schema
    schema.validate(df)


def test_add_boolean_checks_no_true_count():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    additional_check = {FALSE_COUNT_KEY: 3, MARGIN_ERROR_KEY: 0}

    add_boolean_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3  # initial check + 2 added check

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [False, False, False]})

    # Validate the DataFrame against the schema
    schema.validate(df)


def test_add_boolean_checks_no_false_count():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    additional_check = {TRUE_COUNT_KEY: 3, MARGIN_ERROR_KEY: 0}

    add_boolean_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 3  # initial check + 2 added check

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [True, True, True]})

    # Validate the DataFrame against the schema
    schema.validate(df)

    def test_add_numeric_checks():
        schema = DataFrameSchema(
            {
                "col1": Column(float, checks=[Check.greater_than(0)]),
            }
        )

        additional_check = {MEAN_KEY: 5.0, MARGIN_ERROR_KEY: 1.0}

        add_numeric_checks(schema, "col1", additional_check)

        assert len(schema.columns["col1"].checks) == 2  # initial check + 1 added check

        # Create a DataFrame to test the checks
        df = pd.DataFrame({"col1": [4.5, 5.5, 5.0]})

        # Validate the DataFrame against the schema
        schema.validate(df)

    def test_add_numeric_checks_with_decimal_precision():
        schema = DataFrameSchema(
            {
                "col1": Column(float, checks=[Check.greater_than(0)]),
            }
        )

        additional_check = {
            MEAN_KEY: 5.0,
            MARGIN_ERROR_KEY: 1.0,
            DECIMAL_PRECISION_KEY: 2,
        }

        add_numeric_checks(schema, "col1", additional_check)

        assert len(schema.columns["col1"].checks) == 3  # initial check + 2 added checks

        # Create a DataFrame to test the checks
        df = pd.DataFrame({"col1": [4.50, 5.55, 5.00]})

        # Validate the DataFrame against the schema
        schema.validate(df)


def test_add_numeric_checks_no_mean():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )

    additional_check = {MARGIN_ERROR_KEY: 1.0}

    add_numeric_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2  # initial check + 1 added check

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [0.5, 1.5, 1.0]})

    # Validate the DataFrame against the schema
    schema.validate(df)


def test_add_numeric_checks_no_margin_error():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )

    additional_check = {MEAN_KEY: 5.0}

    add_numeric_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2  # initial check + 1 added check

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [5.0, 5.0, 5.0]})

    # Validate the DataFrame against the schema
    schema.validate(df)


def test_add_numeric_checks_no_decimal_precision():
    schema = DataFrameSchema(
        {
            "col1": Column(float, checks=[Check.greater_than(0)]),
        }
    )

    additional_check = {MEAN_KEY: 5.0, MARGIN_ERROR_KEY: 1.0}

    add_numeric_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2  # initial check + 1 added check

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [4.5, 5.5, 5.0]})

    # Validate the DataFrame against the schema
    schema.validate(df)


def test_generate_schema_with_custom_data(generate_json_schema_file):
    checkpoint_name = "test_checkpoint"

    schema = generate_schema(checkpoint_name)

    assert "col1" in schema.columns
    assert "col2" in schema.columns
    assert len(schema.columns["col1"].checks) == 2  # 1 initial check + 1 added checks
    assert len(schema.columns["col2"].checks) == 3  # 1 initial check + 2 added checks

    df = pd.DataFrame({"col1": [4.50, 5.55, 5.00], "col2": [True, True, False]})
    schema.validate(df)


def test_generate_schema_no_pandera_schema_key():
    checkpoint_name = "test_checkpoint"
    schema_json = {
        DATAFRAME_CUSTOM_DATA_KEY: {
            "col1": {TYPE_KEY: "numeric", MEAN_KEY: 5.0, MARGIN_ERROR_KEY: 1.0}
        }
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(schema_json))):
        schema = generate_schema(checkpoint_name)

    assert "col1" not in schema.columns  # No initial schema provided


def test_generate_schema_invalid_json():
    checkpoint_name = "test_checkpoint"
    invalid_json = "{invalid_json}"

    with patch("builtins.open", mock_open(read_data=invalid_json)):
        try:
            generate_schema(checkpoint_name)
        except json.JSONDecodeError:
            assert True
        else:
            assert False

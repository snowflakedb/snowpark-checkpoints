#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
import os
from unittest.mock import ANY, call, patch, mock_open
from numpy import float64

from pytest import raises
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.utils.constant import (
    BOOLEAN_TYPE,
    CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME,
    CHECKPOINT_TABLE_NAME_FORMAT,
    DATAFRAME_CUSTOM_DATA_KEY,
    DEFAULT_KEY,
    EXCEPT_HASH_AGG_QUERY,
    FAIL_STATUS,
    FLOAT_TYPE,
    NAME_KEY,
    OVERWRITE_MODE,
    PASS_STATUS,
    ROWS_COUNT_KEY,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    TYPE_KEY,
)
from pandera import Column, Check, DataFrameSchema
import pandas as pd
from unittest.mock import MagicMock
import numpy as np
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.utils.utils_checks import (
    _compare_data,
    _process_sampling,
    _update_validation_result,
)
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import SamplingStrategy
from pandera import DataFrameSchema, Column, Check

from snowflake.snowpark_checkpoints.utils.utils_checks import (
    _generate_schema,
    _add_numeric_checks,
    MEAN_KEY,
    DECIMAL_PRECISION_KEY,
    _skip_checks_on_schema,
    SKIP_ALL,
    _add_boolean_checks,
    TRUE_COUNT_KEY,
    FALSE_COUNT_KEY,
    MARGIN_ERROR_KEY,
)
from snowflake.snowpark_checkpoints.validation_results import ValidationResult


def test_skip_specific_check():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0), Check.less_than(10)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    skip_checks = {"col1": ["greater_than"]}
    _skip_checks_on_schema(schema, skip_checks)

    assert len(schema.columns["col1"].checks) == 1
    assert schema.columns["col1"].checks[0].name == "less_than"
    assert len(schema.columns["col2"].checks) == 1


def test_skip_checks_on_nonexistent_column():
    schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0), Check.less_than(10)]),
            "col2": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    skip_checks = {"col3": [SKIP_ALL]}
    _skip_checks_on_schema(schema, skip_checks)

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
    _skip_checks_on_schema(schema, skip_checks)

    assert len(schema.columns["col1"].checks) == 2
    assert len(schema.columns["col2"].checks) == 1


def test_add_boolean_checks():
    schema = DataFrameSchema(
        {
            "col1": Column(bool, checks=[Check.isin([True, False])]),
        }
    )

    additional_check = {
        TRUE_COUNT_KEY: 2,
        FALSE_COUNT_KEY: 1,
        MARGIN_ERROR_KEY: 0,
        ROWS_COUNT_KEY: 3,
    }

    _add_boolean_checks(schema, "col1", additional_check)

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

    additional_check = {
        TRUE_COUNT_KEY: 2,
        FALSE_COUNT_KEY: 1,
        MARGIN_ERROR_KEY: 1,
        ROWS_COUNT_KEY: 3,
    }

    _add_boolean_checks(schema, "col1", additional_check)

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

    additional_check = {FALSE_COUNT_KEY: 3, MARGIN_ERROR_KEY: 0, ROWS_COUNT_KEY: 3}

    _add_boolean_checks(schema, "col1", additional_check)

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

    additional_check = {TRUE_COUNT_KEY: 3, MARGIN_ERROR_KEY: 0, ROWS_COUNT_KEY: 3}

    _add_boolean_checks(schema, "col1", additional_check)

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

    _add_numeric_checks(schema, "col1", additional_check)

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

    _add_numeric_checks(schema, "col1", additional_check)

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

    _add_numeric_checks(schema, "col1", additional_check)

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

    _add_numeric_checks(schema, "col1", additional_check)

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

    _add_numeric_checks(schema, "col1", additional_check)

    assert len(schema.columns["col1"].checks) == 2  # initial check + 1 added check

    # Create a DataFrame to test the checks
    df = pd.DataFrame({"col1": [4.5, 5.5, 5.0]})

    # Validate the DataFrame against the schema
    schema.validate(df)


def test_generate_schema_with_custom_data():
    schema = DataFrameSchema(
        {
            "col1": Column(float64, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "col2": Column(bool, Check.isin([True, False])),
        }
    )

    schema_data = {
        "pandera_schema": json.loads(schema.to_json()),
        "custom_data": {
            "columns": [
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
                    "margin_error": 0.0,
                    "rows_count": 3,
                },
            ],
        },
    }

    checkpoint_name = "test_checkpoint"
    current_directory_path = os.getcwd()

    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )

    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path)

    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME.format(
        checkpoint_name
    )

    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)

    with open(checkpoint_file_path, "w") as output_file:
        output_file.write(json.dumps(schema_data))

    schema = _generate_schema(checkpoint_name)

    assert "col1" in schema.columns
    assert "col2" in schema.columns
    assert len(schema.columns["col1"].checks) == 2  # 1 initial check + 1 added checks
    assert len(schema.columns["col2"].checks) == 3  # 1 initial check + 2 added checks

    df = pd.DataFrame({"col1": [4.50, 5.55, 5.00], "col2": [True, True, False]})
    schema.validate(df)


def test_generate_schema_no_pandera_schema_key():
    checkpoint_name = "test_checkpoint"
    schema_json = {
        DATAFRAME_CUSTOM_DATA_KEY: [
            {
                NAME_KEY: "col1",
                TYPE_KEY: "numeric",
                MEAN_KEY: 5.0,
                MARGIN_ERROR_KEY: 1.0,
            }
        ]
    }

    try:
        with patch("builtins.open", mock_open(read_data=json.dumps(schema_json))):
            schema = _generate_schema(checkpoint_name)
            assert False
    except:
        pass


def test_generate_schema_invalid_json():
    checkpoint_name = "test_checkpoint"
    invalid_json = "{invalid_json}"

    with patch("builtins.open", mock_open(read_data=invalid_json)):
        try:
            _generate_schema(checkpoint_name)
        except json.JSONDecodeError:
            assert True
        else:
            assert False


def test_process_sampling_with_default_params():
    # Mock Snowpark DataFrame
    df = MagicMock(spec=SnowparkDataFrame)
    df.write = MagicMock()
    df.write.mode = MagicMock(return_value=df.write)
    df.write.save_as_table = MagicMock()
    df.write = MagicMock()
    df.write.mode = MagicMock(return_value=df.write)
    df.write.save_as_table = MagicMock()
    df.count.return_value = 10

    # Mock Pandera schema
    pandera_schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0)]),
            "col2": Column(float, checks=[Check.less_than(10.0)]),
        }
    )

    # Mock job context
    job_context = MagicMock(spec=SnowparkJobContext)

    # Mock SamplingAdapter
    with patch(
        "snowflake.snowpark_checkpoints.utils.utils_checks.SamplingAdapter"
    ) as MockSamplingAdapter:
        mock_sampler = MockSamplingAdapter.return_value
        mock_sampler.get_sampled_pandas_args.return_value = [
            pd.DataFrame({"col1": [1, 2], "col2": [3.0, 4.0]})
        ]

        # Call the function
        adjusted_schema, sampled_df = _process_sampling(df, pandera_schema, job_context)

        # Assertions
        assert "COL1" in adjusted_schema.columns
        assert "COL2" in adjusted_schema.columns
        assert len(adjusted_schema.columns) == 2
        assert sampled_df.shape == (2, 2)
        assert np.all(sampled_df.index == 1)


def test_process_sampling_with_custom_params():
    # Mock Snowpark DataFrame
    df = MagicMock(spec=SnowparkDataFrame)
    df.count.return_value = pd.Series([10])

    # Mock Pandera schema
    pandera_schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0)]),
            "col2": Column(float, checks=[Check.less_than(10.0)]),
        }
    )

    # Mock job context
    job_context = MagicMock(spec=SnowparkJobContext)

    # Mock SamplingAdapter
    with patch(
        "snowflake.snowpark_checkpoints.utils.utils_checks.SamplingAdapter"
    ) as MockSamplingAdapter:
        mock_sampler = MockSamplingAdapter.return_value
        mock_sampler.get_sampled_pandas_args.return_value = [
            pd.DataFrame({"col1": [1, 2], "col2": [3.0, 4.0]})
        ]

        # Call the function with custom parameters
        adjusted_schema, sampled_df = _process_sampling(
            df,
            pandera_schema,
            job_context,
            sample_frac=0.2,
            sample_number=5,
            sampling_strategy=SamplingStrategy.RANDOM_SAMPLE,
        )

        # Assertions
        assert "COL1" in adjusted_schema.columns
        assert "COL2" in adjusted_schema.columns
        assert len(adjusted_schema.columns) == 2
        assert sampled_df.shape == (2, 2)
        assert np.all(sampled_df.index == 1)


def test_process_sampling_no_job_context():
    # Mock Snowpark DataFrame
    df = MagicMock(spec=SnowparkDataFrame)
    df.count.return_value = pd.Series([10])

    # Mock Pandera schema
    pandera_schema = DataFrameSchema(
        {
            "col1": Column(int, checks=[Check.greater_than(0)]),
            "col2": Column(float, checks=[Check.less_than(10.0)]),
        }
    )

    # Mock SamplingAdapter
    with patch(
        "snowflake.snowpark_checkpoints.utils.utils_checks.SamplingAdapter"
    ) as MockSamplingAdapter:
        mock_sampler = MockSamplingAdapter.return_value
        mock_sampler.get_sampled_pandas_args.return_value = [
            pd.DataFrame({"col1": [1, 2], "col2": [3.0, 4.0]})
        ]

        # Call the function without job context
        adjusted_schema, sampled_df = _process_sampling(df, pandera_schema)

        # Assertions
        assert "COL1" in adjusted_schema.columns
        assert "COL2" in adjusted_schema.columns
        assert len(adjusted_schema.columns) == 2
        assert sampled_df.shape == (2, 2)
        assert np.all(sampled_df.index == 1)


def test_compare_data_match():
    checkpoint_name = "test_checkpoint"
    new_checkpoint_name = CHECKPOINT_TABLE_NAME_FORMAT.format(checkpoint_name)

    # Mock Snowpark DataFrame
    df = MagicMock(spec=SnowparkDataFrame)

    # Mock job context
    job_context = MagicMock(spec=SnowparkJobContext)
    session = MagicMock()
    job_context.snowpark_session = session

    # Mock session.sql to return an empty DataFrame (indicating no mismatch)
    session.sql.return_value.count.return_value = 0

    checkpoint_name = "test_checkpoint"
    validation_status = PASS_STATUS

    with (
        patch("os.getcwd", return_value="/mocked/path"),
        patch("os.path.exists", return_value=False),
        patch("builtins.open", mock_open()),
        patch("json.dump"),
        patch(
            "snowflake.snowpark_checkpoints.utils.utils_checks._update_validation_result"
        ) as mock_update_validation_result,
    ):
        # Call the function
        _compare_data(df, job_context, checkpoint_name)

    # Assertions
    mock_update_validation_result.assert_called_once_with(
        checkpoint_name, validation_status
    )
    df.write.save_as_table.assert_called_once_with(
        table_name=new_checkpoint_name, mode=OVERWRITE_MODE
    )
    calls = [
        call(EXCEPT_HASH_AGG_QUERY, [checkpoint_name, new_checkpoint_name]),
        call().count(),
    ]
    session.sql.assert_has_calls(calls)
    job_context.mark_pass.assert_called_once_with(checkpoint_name)
    job_context.mark_fail.assert_not_called()


def test_compare_data_mismatch():
    checkpoint_name = "test_checkpoint"
    new_checkpoint_name = CHECKPOINT_TABLE_NAME_FORMAT.format(checkpoint_name)

    # Mock Snowpark DataFrame
    df = MagicMock(spec=SnowparkDataFrame)

    # Mock job context
    job_context = MagicMock(spec=SnowparkJobContext)
    session = MagicMock()
    job_context.snowpark_session = session
    job_context.job_name = checkpoint_name

    # Mock session.sql to return a non-empty DataFrame (indicating a mismatch)
    session.sql.return_value.count.return_value = 1

    with (
        patch("os.getcwd", return_value="/mocked/path"),
        patch("os.path.exists", return_value=False),
        patch("builtins.open", mock_open()),
        patch("json.dump"),
        patch(
            "snowflake.snowpark_checkpoints.utils.utils_checks._update_validation_result"
        ) as mock_update_validation_result,
    ):
        # Call the function and expect a SchemaValidationError
        with raises(
            SchemaValidationError,
            match=f"Data mismatch for checkpoint {checkpoint_name}",
        ):
            _compare_data(df, job_context, checkpoint_name)

    # Assertions
    mock_update_validation_result.assert_called_once_with(checkpoint_name, FAIL_STATUS)
    df.write.save_as_table.assert_called_once_with(
        table_name=new_checkpoint_name, mode=OVERWRITE_MODE
    )
    calls = [
        call(EXCEPT_HASH_AGG_QUERY, [checkpoint_name, new_checkpoint_name]),
        call().count(),
    ]
    session.sql.assert_has_calls(calls)
    job_context.mark_fail.assert_called()
    job_context.mark_pass.assert_not_called()


def test_update_validation_result_with_file():
    checkpoint_name = "test_checkpoint"
    validation_status = PASS_STATUS

    with (
        patch(
            "snowflake.snowpark_checkpoints.utils.utils_checks.ValidationResultsMetadata"
        ) as MockValidationResultsMetadata,
        patch(
            "snowflake.snowpark_checkpoints.utils.utils_checks.datetime"
        ) as mock_datetime,
    ):
        mock_datetime.now.return_value.isoformat.return_value = "2021-01-01T00:00:00"
        mock_pipeline_result_metadata = MockValidationResultsMetadata.return_value

        # Call the function
        _update_validation_result(checkpoint_name, validation_status)

        # Assertions
        MockValidationResultsMetadata.assert_called_once()

        mock_pipeline_result_metadata.add_validation_result.assert_called_once_with(
            ValidationResult(
                timestamp="2021-01-01T00:00:00",
                file=DEFAULT_KEY,
                line_of_code=-1,
                checkpoint_name=checkpoint_name,
                result=validation_status,
            )
        )
        mock_pipeline_result_metadata.save.assert_called_once()


def test_update_validation_result_without_file():
    checkpoint_name = "test_checkpoint"
    validation_status = PASS_STATUS

    with (
        patch(
            "snowflake.snowpark_checkpoints.utils.utils_checks.ValidationResultsMetadata"
        ) as MockValidationResultsMetadata,
        patch(
            "snowflake.snowpark_checkpoints.utils.utils_checks.inspect.stack",
            return_value=[
                MagicMock(
                    filename="test_file.py",
                    code_context=["_check_dataframe_schema_file"],
                ),
                MagicMock(
                    filename="test_file.py",
                    code_context=["validate_dataframe_checkpoint"],
                    lineno=1,
                ),
            ],
        ),
        patch(
            "snowflake.snowpark_checkpoints.utils.utils_checks.datetime"
        ) as mock_datetime,
    ):
        mock_datetime.now.return_value.isoformat.return_value = "2021-01-01T00:00:00"
        mock_pipeline_result_metadata = MockValidationResultsMetadata.return_value

        # Call the function
        _update_validation_result(checkpoint_name, validation_status)

        # Assertions
        MockValidationResultsMetadata.assert_called_once()
        mock_pipeline_result_metadata.add_validation_result.assert_called_once_with(
            ValidationResult(
                timestamp="2021-01-01T00:00:00",
                file="test_file.py",
                line_of_code=1,
                checkpoint_name=checkpoint_name,
                result=validation_status,
            )
        )
        mock_pipeline_result_metadata.save.assert_called_once()

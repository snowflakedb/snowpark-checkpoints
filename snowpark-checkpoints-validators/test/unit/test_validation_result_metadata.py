import os
from unittest.mock import mock_open, patch
from pytest import fixture
from snowflake.snowpark_checkpoints.singleton import Singleton
from snowflake.snowpark_checkpoints.utils.constants import (
    PASS_STATUS,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    VALIDATION_RESULTS_JSON_FILE_NAME,
)
from snowflake.snowpark_checkpoints.validation_result_metadata import (
    ValidationResultsMetadata,
)
from snowflake.snowpark_checkpoints.validation_results import (
    ValidationResult,
    ValidationResults,
)
from pandas import DataFrame as PandasDataFrame, testing as PandasTesting
from pandera import DataFrameSchema, Column, Check
from snowflake.snowpark_checkpoints.checkpoint import _validate


@fixture()
def sample_data():
    df = PandasDataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})

    valid_schema = DataFrameSchema(
        {
            "column1": Column(int, Check(lambda x: x > 0)),
            "column2": Column(str, Check(lambda x: x.isin(["a", "b", "c"]))),
        }
    )

    invalid_schema = DataFrameSchema(
        {
            "column1": Column(int, Check(lambda x: x > 3)),
            "column2": Column(str, Check(lambda x: x.isin(["d", "e", "f"]))),
        }
    )

    return df, valid_schema, invalid_schema


@fixture(autouse=True)
def singleton():
    Singleton._instances = {}


def test_load_with_valid_file():
    test_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(
        test_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        VALIDATION_RESULTS_JSON_FILE_NAME,
    )
    with open(result_path) as file:
        validation_result_json = file.read()
        mock_validation_results = ValidationResults.model_validate_json(
            validation_result_json
        )

    metadata = ValidationResultsMetadata(test_path)

    assert metadata.validation_results == mock_validation_results


def test_load_with_no_file():
    path = "mock_path"
    metadata = ValidationResultsMetadata(path)

    assert metadata.validation_results == ValidationResults(results=[])


def test_add_validation_result():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path,
        "checkpoint_validation_results",
        "non_existent_file.json",
    )

    metadata = ValidationResultsMetadata(path)

    new_validation_result = ValidationResult(
        timestamp="2021-01-01T00:00:00",
        file="file2",
        line_of_code=1,
        result=PASS_STATUS,
        checkpoint_name="checkpoint2",
    )

    metadata.add_validation_result(new_validation_result)

    assert metadata.validation_results == ValidationResults(
        results=[new_validation_result]
    )


def test_save_success():
    test_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(
        test_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        VALIDATION_RESULTS_JSON_FILE_NAME,
    )
    with open(result_path) as file:
        validation_result_json = file.read()
        mock_validation_results = ValidationResults.model_validate_json(
            validation_result_json
        )

    metadata = ValidationResultsMetadata(test_path)
    with patch("builtins.open", mock_open()) as mock_open_file:
        metadata.save()

    mock_open_file().write.assert_called_once_with(
        mock_validation_results.model_dump_json()
    )


def test_save_success():
    test_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(
        test_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        VALIDATION_RESULTS_JSON_FILE_NAME,
    )
    with open(result_path) as file:
        validation_result_json = file.read()
        mock_validation_results = ValidationResults.model_validate_json(
            validation_result_json
        )

    metadata = ValidationResultsMetadata(test_path)
    with patch("builtins.open", mock_open()) as mock_open_file:
        metadata.save()

    mock_open_file().write.assert_called_once_with(
        mock_validation_results.model_dump_json()
    )


def test_save_creates_directory():
    test_path = os.path.dirname(os.path.abspath(__file__))
    metadata = ValidationResultsMetadata(test_path)

    with (
        patch(
            "snowflake.snowpark_checkpoints.io_utils.io_default_strategy.IODefaultStrategy.folder_exists",
            return_value=False,
        ),
        patch("os.makedirs") as mock_makedirs,
        patch("builtins.open", mock_open()),
    ):
        metadata.save()

    mock_makedirs.assert_called_once_with(
        metadata.validation_results_directory, exist_ok=False
    )


def test_clean_with_existing_file():
    test_path = os.path.dirname(os.path.abspath(__file__))
    result_path = os.path.join(
        test_path,
        SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        VALIDATION_RESULTS_JSON_FILE_NAME,
    )
    with open(result_path) as file:
        validation_result_json = file.read()
        mock_validation_results = ValidationResults.model_validate_json(
            validation_result_json
        )

    metadata = ValidationResultsMetadata(test_path)
    metadata.validation_results = mock_validation_results

    with patch("os.path.exists", return_value=True):
        metadata.clean()

    assert metadata.validation_results == mock_validation_results


def test_clean_with_no_file():
    test_path = os.path.dirname(os.path.abspath(__file__))
    metadata = ValidationResultsMetadata(test_path)

    new_validation_result = ValidationResult(
        timestamp="2021-01-01T00:00:00",
        file="file2",
        line_of_code=1,
        result=PASS_STATUS,
        checkpoint_name="checkpoint2",
    )
    metadata.add_validation_result(new_validation_result)

    with patch(
        "snowflake.snowpark_checkpoints.io_utils.io_default_strategy.IODefaultStrategy.file_exists",
        return_value=False,
    ):
        metadata.clean()

    assert metadata.validation_results == ValidationResults(results=[])


def test_validate_valid_schema(sample_data):
    df, valid_schema, _ = sample_data
    is_valid, result = _validate(valid_schema, df)
    assert is_valid
    assert isinstance(result, PandasDataFrame)
    PandasTesting.assert_frame_equal(result, df)


def test_validate_invalid_schema(sample_data):
    df, _, invalid_schema = sample_data
    is_valid, result = _validate(invalid_schema, df)
    assert not is_valid
    assert isinstance(result, PandasDataFrame)
    assert "failure_case" in result.columns

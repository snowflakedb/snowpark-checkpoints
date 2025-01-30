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
        patch("os.path.exists", return_value=False),
        patch("os.makedirs") as mock_makedirs,
        patch("builtins.open", mock_open()),
    ):
        metadata.save()

    mock_makedirs.assert_called_once_with(metadata.validation_results_directory)


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

    with patch("os.path.exists", return_value=False):
        metadata.clean()

    assert metadata.validation_results == ValidationResults(results=[])

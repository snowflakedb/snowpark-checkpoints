import json
import os
from unittest.mock import mock_open, patch
from snowflake.snowpark_checkpoints.utils.constant import (
    PASS_STATUS,
    VALIDATION_RESULTS_JSON_FILE_NAME,
)
from snowflake.snowpark_checkpoints.validation_result_metadata import (
    ValidationResultsMetadata,
)
from snowflake.snowpark_checkpoints.validation_results import (
    ValidationResult,
    ValidationResults,
)


def test_load_with_valid_file():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path, "checkpoint_validation_results", VALIDATION_RESULTS_JSON_FILE_NAME
    )
    with open(path) as file:
        validation_result_json = file.read()
        mock_validation_results = ValidationResults.model_validate_json(
            validation_result_json
        )

    metadata = ValidationResultsMetadata(path)

    assert metadata.validation_results == mock_validation_results


def test_load_with_no_file():
    metadata = ValidationResultsMetadata()

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

    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path, "checkpoint_validation_results", VALIDATION_RESULTS_JSON_FILE_NAME
    )
    with open(path) as file:
        mock_validation_results = file.read()

    metadata = ValidationResultsMetadata(path)
    with patch("builtins.open", mock_open()) as mock_open_file:
        metadata.save()

    mock_open_file().write.assert_called_once_with(mock_validation_results)

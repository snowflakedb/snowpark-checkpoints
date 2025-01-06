import json
import os
from unittest.mock import mock_open, patch
from snowflake.snowpark_checkpoints.utils.constant import (
    FAIL_STATUS,
    PASS_STATUS,
    VALIDATION_RESULTS_JSON_FILE_NAME,
)
from snowflake.snowpark_checkpoints.validation_result_metadata import (
    PipelineResultMetadata,
)
from snowflake.snowpark_checkpoints.validation_results import (
    ValidationResult,
    as_validation_result,
)


def test_load_with_valid_file():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path, "checkpoint_validation_results", VALIDATION_RESULTS_JSON_FILE_NAME
    )
    with open(path) as file:
        mock_validation_results = json.load(file, object_hook=as_validation_result)

    metadata = PipelineResultMetadata(path)

    assert metadata.pipeline_result == mock_validation_results


def test_load_with_no_file():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, "checkpoint_validation_results", "non_existent_file.json")

    metadata = PipelineResultMetadata(path)

    assert metadata.pipeline_result == {}


def test_update_validation_result_new_file():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path,
        "checkpoint_validation_results",
        "non_existent_file.json",
    )

    metadata = PipelineResultMetadata()

    new_validation_result = ValidationResult(
        result=PASS_STATUS,
        file="file2",
        line_of_code=1,
        timestamp="2021-01-01T00:00:00",
    )

    metadata.update_validation_result("checkpoint1", new_validation_result)

    assert "file2" in metadata.pipeline_result
    assert "checkpoint1" in metadata.pipeline_result["file2"]
    assert metadata.pipeline_result["file2"]["checkpoint1"] == [new_validation_result]


def test_update_validation_result_existing_file_new_checkpoint():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path, "checkpoint_validation_results", VALIDATION_RESULTS_JSON_FILE_NAME
    )
    file_name = "path/to/your/test_file_1.py"
    checkpoint_name = "checkpoint1"

    metadata = PipelineResultMetadata(path)

    new_validation_result = ValidationResult(
        result=PASS_STATUS,
        file=file_name,
        line_of_code=1,
        timestamp="2021-01-01T00:00:00",
    )

    metadata.update_validation_result(checkpoint_name, new_validation_result)

    assert file_name in metadata.pipeline_result
    assert checkpoint_name in metadata.pipeline_result[file_name]
    assert metadata.pipeline_result[file_name][checkpoint_name] == [
        new_validation_result
    ]


def test_update_validation_result_existing_file_existing_checkpoint():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path, "checkpoint_validation_results", VALIDATION_RESULTS_JSON_FILE_NAME
    )
    file_name = "path/to/your/test_file_1.py"
    checkpoint_name = "test_case_1"

    metadata = PipelineResultMetadata(path)

    new_validation_result = ValidationResult(
        result=FAIL_STATUS,
        file=file_name,
        line_of_code=1,
        timestamp="2021-01-01T00:00:00",
    )

    metadata.update_validation_result(checkpoint_name, new_validation_result)

    assert file_name in metadata.pipeline_result
    assert checkpoint_name in metadata.pipeline_result[file_name]
    assert len(metadata.pipeline_result[file_name][checkpoint_name]) == 2
    assert (
        metadata.pipeline_result[file_name][checkpoint_name][1] == new_validation_result
    )


def test_save_success():

    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        path, "checkpoint_validation_results", VALIDATION_RESULTS_JSON_FILE_NAME
    )
    with open(path) as file:
        mock_validation_results = file.read()
        # remove the last end of line character
        mock_validation_results = mock_validation_results[:-1]

    metadata = PipelineResultMetadata(path)
    with patch("builtins.open", mock_open()) as mock_open_file:
        metadata.save()

    mock_open_file().write.assert_called_once_with(mock_validation_results)

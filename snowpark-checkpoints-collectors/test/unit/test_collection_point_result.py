import json

from deepdiff import DeepDiff

from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
    CollectionResult,
)
from snowflake.snowpark_checkpoints_collector.collection_result.model.collection_point_result import (
    TIMESTAMP_KEY,
    FILE_KEY,
)

RESULT_DATA_EXPECTED = (
    '{"timestamp": "2024-12-20 14:33:05", "file": "unit/test_collection_point_result.py", '
    '"line_of_code": 2, "checkpoint_name": "checkpoint_test", "result": "PASS"}'
)


def generate_collection_point_result_object():
    file_path = __file__
    checkpoint_name = "checkpoint_test"
    line_of_code = 2
    collection_result = CollectionPointResult(file_path, line_of_code, checkpoint_name)
    return collection_result


def test_get_collection_point_result_object():
    expected_file_path = __file__
    expected_checkpoint_name = "checkpoint_test"

    collection_result = generate_collection_point_result_object()

    assert collection_result._file_path == expected_file_path
    assert collection_result._checkpoint_name == expected_checkpoint_name
    assert collection_result._timestamp is not None
    assert collection_result.result is None
    assert collection_result._line_of_code == 2


def test_set_collection_point_result_to_pass():
    collection_result = generate_collection_point_result_object()
    collection_result.result = CollectionResult.PASS
    assert collection_result.result == CollectionResult.PASS


def test_set_collection_point_result_to_fail():
    collection_result = generate_collection_point_result_object()
    collection_result.result = CollectionResult.FAIL
    assert collection_result.result == CollectionResult.FAIL


def test_get_collection_result_data():
    expected_collection_result_data = json.loads(RESULT_DATA_EXPECTED)
    collection_result = generate_collection_point_result_object()
    collection_result.result = CollectionResult.PASS
    collection_result_data = collection_result.get_collection_result_data()

    file_path_to_ignore = f"root['{FILE_KEY}']"
    timestamp_path_to_ignore = f"root['{TIMESTAMP_KEY}']"

    diff = DeepDiff(
        expected_collection_result_data,
        collection_result_data,
        ignore_order=True,
        exclude_paths=[file_path_to_ignore, timestamp_path_to_ignore],
    )

    assert diff == {}

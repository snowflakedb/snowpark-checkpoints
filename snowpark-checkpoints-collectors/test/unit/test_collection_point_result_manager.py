import json
from unittest import mock

import pytest
from deepdiff import DeepDiff

from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
    CollectionPointResultManager,
)
from snowflake.snowpark_checkpoints_collector.collection_result.model.collection_point_result import (
    TIMESTAMP_KEY,
    FILE_KEY,
    CollectionResult,
)

from snowflake.snowpark_checkpoints_collector.singleton import Singleton
from snowflake.snowpark_checkpoints_collector.collection_result.model.collection_point_result_manager import (
    RESULTS_KEY,
)

EXPECTED_MODEL = (
    '{"timestamp": "2024-12-20 14:50:49", "file": "unit/test_collection_point_result_manager.py", '
    '"line_of_code": 10, "checkpoint_name": "checkpoint_test", "result": "PASS"}'
)


@pytest.fixture
def singleton():
    Singleton._instances = {}


def generate_collection_point_result_object():
    file_path = __file__
    checkpoint_name = "checkpoint_test"
    line_of_code = 10
    collection_result = CollectionPointResult(file_path, line_of_code, checkpoint_name)
    collection_result.result = CollectionResult.PASS
    return collection_result


def test_add_result(singleton):
    manager = CollectionPointResultManager()
    collection_result = generate_collection_point_result_object()

    with mock.patch("builtins.open") as mock_open:
        manager.add_result(collection_result)
        mock_open.assert_called()

    model_json = manager.to_json()
    model = json.loads(model_json)
    collection_point_dict = model[RESULTS_KEY][0]

    timestamp_path_to_ignore = f"root['{TIMESTAMP_KEY}']"
    file_path_to_ignore = f"root['{FILE_KEY}']"

    expected_collection_point_dict = json.loads(EXPECTED_MODEL)

    diff = DeepDiff(
        expected_collection_point_dict,
        collection_point_dict,
        ignore_order=True,
        exclude_paths=[timestamp_path_to_ignore, file_path_to_ignore],
    )

    assert diff == {}

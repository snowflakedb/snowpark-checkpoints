import json
from unittest import mock

import pytest
from deepdiff import DeepDiff
from pyspark.sql.types import StructType, StructField, StringType

from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
    CollectionPointResultManager,
)
from snowflake.snowpark_checkpoints_collector.collection_result.model.collection_point_result import (
    TIMESTAMP_KEY,
    FILE_KEY,
)
from snowflake.snowpark_checkpoints_collector.utils import file_utils
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton

EXPECTED_MODEL = (
    '{"timestamp": "2024-12-19 14:31:44", "schema_df": {"type": "struct", "fields": [{"name": "field1", '
    '"type": "string", "nullable": false, "metadata": {}}]}, "result": "PASS", "line_of_code": 10}'
)


@pytest.fixture
def singleton():
    Singleton._instances = {}


def generate_collection_point_result_object():
    file_path = __file__
    checkpoint_name = "checkpoint_test"
    schema = StructType([StructField("field1", StringType(), False)])
    line_of_code = 10
    collection_result = CollectionPointResult(
        file_path, checkpoint_name, schema, line_of_code
    )
    collection_result.set_collection_point_result_to_pass()
    return collection_result


def test_add_result(singleton):
    manager = CollectionPointResultManager()
    collection_result = generate_collection_point_result_object()

    with mock.patch("builtins.open") as mock_open:
        manager.add_result(collection_result)
        mock_open.assert_called()

    model_json = manager.to_json()
    model_dict = json.loads(model_json)

    relative_path = file_utils.get_relative_file_path(__file__)
    checkpoint_name = collection_result.checkpoint_name

    assert model_dict.get(relative_path) is not None
    assert model_dict[relative_path].get(checkpoint_name) is not None

    file_path_to_ignore = f"root['{FILE_KEY}']"
    timestamp_path_to_ignore = f"root['{TIMESTAMP_KEY}']"

    collection_result_data = model_dict[relative_path][checkpoint_name][0]
    expected_collection_result_data = json.loads(EXPECTED_MODEL)

    diff = DeepDiff(
        expected_collection_result_data,
        collection_result_data,
        ignore_order=True,
        exclude_paths=[file_path_to_ignore, timestamp_path_to_ignore],
    )

    assert diff == {}

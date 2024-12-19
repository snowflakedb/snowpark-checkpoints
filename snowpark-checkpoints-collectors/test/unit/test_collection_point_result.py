import json

from deepdiff import DeepDiff
from pyspark.sql.types import StructType, StructField, StringType

from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
    CollectionResult,
)
from snowflake.snowpark_checkpoints_collector.collection_result.model.collection_point_result import (
    TIMESTAMP_KEY,
)

RESULT_DATA_EXPECTED = (
    '{"timestamp": "2024-12-19 14:36:05", "schema_df": {"type": "struct", "fields": [{"name": '
    '"field1", "type": "string", "nullable": false, "metadata": {}}]}, "result": "PASS", '
    '"line_of_code": 2}'
)


def generate_collection_point_result_object():
    file_path = __file__
    checkpoint_name = "checkpoint_test"
    schema = StructType([StructField("field1", StringType(), False)])
    line_of_code = 2
    collection_result = CollectionPointResult(
        file_path, checkpoint_name, schema, line_of_code
    )
    return collection_result


def test_get_collection_point_result_object():
    expected_file_path = __file__
    expected_checkpoint_name = "checkpoint_test"
    schema = StructType([StructField("field1", StringType(), False)])

    collection_result = generate_collection_point_result_object()

    assert collection_result.file_path == expected_file_path
    assert collection_result.checkpoint_name == expected_checkpoint_name
    assert collection_result.timestamp is not None
    assert collection_result.schema_df.jsonValue() == schema.jsonValue()
    assert collection_result.result is None
    assert collection_result.line_of_code == 2


def test_set_collection_point_result_to_pass():
    collection_result = generate_collection_point_result_object()
    collection_result.set_collection_point_result_to_pass()
    assert collection_result.result == CollectionResult.PASS


def test_set_collection_point_result_to_fail():
    collection_result = generate_collection_point_result_object()
    collection_result.set_collection_point_result_to_fail()
    assert collection_result.result == CollectionResult.FAIL


def test_get_collection_result_data():
    expected_collection_result_data = json.loads(RESULT_DATA_EXPECTED)
    collection_result = generate_collection_point_result_object()
    collection_result.set_collection_point_result_to_pass()
    collection_result_data = collection_result.get_collection_result_data()

    timestamp_path_to_ignore = f"root['{TIMESTAMP_KEY}']"

    diff = DeepDiff(
        expected_collection_result_data,
        collection_result_data,
        ignore_order=True,
        exclude_paths=[timestamp_path_to_ignore],
    )

    assert diff == {}

import json

from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
)
from snowflake.snowpark_checkpoints_collector.utils import file_utils
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton


class CollectionPointResultManager(metaclass=Singleton):
    def __init__(self) -> None:
        self.result_collection: dict[str, dict[str, list[any]]] = {}
        self.output_file_path = file_utils.get_output_file_path()

    def add_result(self, result: CollectionPointResult) -> None:
        relative_path = file_utils.get_relative_file_path(result.file_path)

        if self.result_collection.get(relative_path) is None:
            self.result_collection[relative_path] = {}

        result_data = result.get_collection_result_data()

        if self.result_collection[relative_path].get(result.checkpoint_name) is None:
            self.result_collection[relative_path][result.checkpoint_name] = []

        self.result_collection[relative_path][result.checkpoint_name].append(
            result_data
        )
        self._save_result()

    def to_json(self) -> str:
        result_collection_json = json.dumps(self.result_collection)
        return result_collection_json

    def _save_result(self) -> None:
        result_collection_json = self.to_json()
        with open(self.output_file_path, "w") as f:
            f.write(result_collection_json)

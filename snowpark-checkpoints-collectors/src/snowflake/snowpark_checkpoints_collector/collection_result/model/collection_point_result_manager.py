import json

from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
)
from snowflake.snowpark_checkpoints_collector.utils import file_utils
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton


class CollectionPointResultManager(metaclass=Singleton):

    """Class for manage the checkpoint collection results. It is a singleton.

    Attributes:
        result_collection (dict[str, dict[str, list[any]]]): the collection of the checkpoint results.
        output_file_path (str): the full path of the output file.

    """

    def __init__(self) -> None:
        """Init CollectionPointResultManager."""
        self.result_collection: list[any] = []
        self.output_file_path = file_utils.get_output_file_path()

    def add_result(self, result: CollectionPointResult) -> None:
        """Add the CollectionPointResult result to the collection.

        Args:
            result (CollectionPointResult): the CollectionPointResult to add.

        """
        result_json = result.get_collection_result_data()
        self.result_collection.append(result_json)
        self._save_result()

    def to_json(self) -> str:
        """Convert to json the checkpoint results collected.

        Returns:
            str: the results as json string.

        """
        result_collection_json = json.dumps(self.result_collection)
        return result_collection_json

    def _save_result(self) -> None:
        result_collection_json = self.to_json()
        with open(self.output_file_path, "w") as f:
            f.write(result_collection_json)

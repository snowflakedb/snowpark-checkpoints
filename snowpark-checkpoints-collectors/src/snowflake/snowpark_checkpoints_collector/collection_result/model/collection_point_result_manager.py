#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import json

from typing import Optional

from snowflake.snowpark_checkpoints_collector.collection_result.model import (
    CollectionPointResult,
)
from snowflake.snowpark_checkpoints_collector.singleton import Singleton
from snowflake.snowpark_checkpoints_collector.utils import file_utils


RESULTS_KEY = "results"


class CollectionPointResultManager(metaclass=Singleton):

    """Class for manage the checkpoint collection results. It is a singleton.

    Attributes:
        result_collection (list[any]): the collection of the checkpoint results.
        output_file_path (str): the full path of the output file.

    """

    def __init__(self, output_path: Optional[str] = None) -> None:
        """Init CollectionPointResultManager."""
        self.result_collection: list[any] = []
        self.output_file_path = file_utils.get_output_file_path(output_path)

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
        dict_object = {RESULTS_KEY: self.result_collection}
        result_collection_json = json.dumps(dict_object)
        return result_collection_json

    def _save_result(self) -> None:
        result_collection_json = self.to_json()
        with open(self.output_file_path, "w") as f:
            f.write(result_collection_json)

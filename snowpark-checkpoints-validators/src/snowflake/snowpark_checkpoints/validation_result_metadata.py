import json
import os

from typing import Optional

from snowflake.snowpark_checkpoints.utils.constant import (
    VALIDATION_RESULTS_JSON_FILE_NAME,
)
from snowflake.snowpark_checkpoints.validation_results import (
    ValidationResult,
    ValidationResultEncoder,
    as_validation_result,
)


class PipelineResultMetadata:

    """ValidationResultMetadata class.

    This is a singleton class that reads the validation_results.json file
    and provides an interface to get the validation result configuration.

    Args:
        metaclass (Singleton, optional): Defaults to Singleton.

    """

    def __init__(self, path: Optional[str] = None):
        self._load(path)

    def _load(self, path: Optional[str] = None):
        directory = path if path is not None else os.getcwd()

        self.pipeline_result = {}

        validation_results_file = os.path.join(
            directory, VALIDATION_RESULTS_JSON_FILE_NAME
        )
        if os.path.exists(validation_results_file):
            with open(validation_results_file) as file:
                try:
                    self.pipeline_result = json.load(
                        file, object_hook=as_validation_result
                    )
                except Exception as e:
                    raise Exception(
                        f"Error reading validation results file: {validation_results_file} \n {e}"
                    ) from None

    def update_validation_result(
        self, file_name: str, checkpoint_name: str, validation_result: ValidationResult
    ):

        file_result = self.pipeline_result.get(file_name, {})
        checkpoint_results = file_result.get(checkpoint_name, [])

        checkpoint_results.append(validation_result)

        file_result[checkpoint_name] = checkpoint_results

        self.pipeline_result[file_name] = file_result

    def save(self):
        validation_results_file = os.path.join(
            os.getcwd(), VALIDATION_RESULTS_JSON_FILE_NAME
        )

        with open(validation_results_file, "w") as output_file:
            output_file.write(
                json.dumps(self.pipeline_result, cls=ValidationResultEncoder)
            )

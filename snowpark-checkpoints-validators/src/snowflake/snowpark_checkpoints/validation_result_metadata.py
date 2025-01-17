#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os

from typing import Optional

from snowflake.snowpark_checkpoints.utils.constant import (
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    VALIDATION_RESULTS_JSON_FILE_NAME,
)
from snowflake.snowpark_checkpoints.validation_results import (
    ValidationResult,
    ValidationResults,
)


class ValidationResultsMetadata:

    """ValidationResultsMetadata is a class that manages the loading, storing, and updating of validation results.

    Attributes:
        validation_results (list): A list to store validation results.
        validation_results_file (str): The path to the validation results file.

    Methods:
        __init__(path: Optional[str] = None):
            Initializes the PipelineResultMetadata instance and loads validation results from a JSON file
            if a path is provided.
        _load(path: Optional[str] = None):
            Loads validation results from a JSON file. If no path is provided, the current working directory is used.
        add_validation_result(validation_result: dict):
            Adds a validation result to the pipeline result list.
        save():
            Saves the validation results to a JSON file in the current working directory.

    """

    def __init__(self, path: Optional[str] = None):
        self._load(path)

    def _load(self, path: Optional[str] = None):
        """Load validation results from a JSON file.

        Args:
            path (Optional[str]): The directory path where the validation results file is located.
                                  If not provided, the current working directory is used.

        Raises:
            Exception: If there is an error reading the validation results file.

        """
        dir_path = path if path else os.getcwd()

        self.validation_results_file = os.path.join(
            dir_path,
            SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
            VALIDATION_RESULTS_JSON_FILE_NAME,
        )

        self.validation_results = ValidationResults(results=[])

        if os.path.exists(self.validation_results_file):
            with open(self.validation_results_file) as file:
                try:
                    validation_result_json = file.read()
                    self.validation_results = ValidationResults.model_validate_json(
                        validation_result_json
                    )
                except Exception as e:
                    raise Exception(
                        f"Error reading validation results file: {self.validation_results_file} \n {e}"
                    ) from None

    def add_validation_result(self, validation_result: ValidationResult):
        """Add a validation result to the pipeline result list.

        Args:
            checkpoint_name (str): The name of the checkpoint.
            validation_result (dict): The validation result to be added.

        """
        self.validation_results.results.append(validation_result)

    def save(self):
        """Save the validation results to a JSON file.

        The file is saved in the current working directory with a predefined
        file name. The validation results are serialized using the
        ValidationResultEncoder.

        Raises:
            OSError: If the file cannot be opened or written to.

        """
        if not os.path.exists(SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME):
            os.makedirs(SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME)

        with open(self.validation_results_file, "w") as output_file:
            output_file.write(self.validation_results.model_dump_json())

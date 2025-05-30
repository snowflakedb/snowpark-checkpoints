# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from typing import Optional

from snowflake.snowpark_checkpoints.io_utils.io_file_manager import get_io_file_manager
from snowflake.snowpark_checkpoints.singleton import Singleton
from snowflake.snowpark_checkpoints.utils.constants import (
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
    VALIDATION_RESULTS_JSON_FILE_NAME,
)
from snowflake.snowpark_checkpoints.validation_results import (
    ValidationResult,
    ValidationResults,
)


LOGGER = logging.getLogger(__name__)


class ValidationResultsMetadata(metaclass=Singleton):

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
        self.validation_results_directory = (
            path if path else get_io_file_manager().getcwd()
        )
        self.validation_results_directory = os.path.join(
            self.validation_results_directory,
            SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
        )

        LOGGER.debug(
            "Setting validation results directory to: '%s'",
            self.validation_results_directory,
        )

        self.validation_results_file = os.path.join(
            self.validation_results_directory,
            VALIDATION_RESULTS_JSON_FILE_NAME,
        )

        LOGGER.debug(
            "Setting validation results file to: '%s'", self.validation_results_file
        )

        self.validation_results = ValidationResults(results=[])

        if get_io_file_manager().file_exists(self.validation_results_file):
            LOGGER.info(
                "Loading validation results from: '%s'", self.validation_results_file
            )
            try:
                validation_result_json = get_io_file_manager().read(
                    self.validation_results_file
                )
                self.validation_results = ValidationResults.model_validate_json(
                    validation_result_json
                )
            except Exception as e:
                raise Exception(
                    f"Error reading validation results file: {self.validation_results_file} \n {e}"
                ) from None
        else:
            LOGGER.info(
                "Validation results file not found: '%s'",
                self.validation_results_file,
            )

    def clean(self):
        """Clean the validation results list.

        This method empties the validation results list.

        """
        if not get_io_file_manager().file_exists(self.validation_results_file):
            LOGGER.info("Cleaning validation results...")
            self.validation_results.results = []

    def add_validation_result(self, validation_result: ValidationResult):
        """Add a validation result to the pipeline result list.

        Args:
            validation_result (dict): The validation result to be added.

        """
        self.validation_results.results.append(validation_result)

    def save(self):
        """Save the validation results to a file.

        This method checks if the directory specified by validation results directory
        exists, and if not, it creates the directory. Then, it writes the validation results
        to a file specified by validation results file in JSON format.

        Raises:
            OSError: If the directory cannot be created or the file cannot be written.

        """
        if not get_io_file_manager().folder_exists(self.validation_results_directory):
            LOGGER.debug(
                "Validation results directory '%s' does not exist. Creating it...",
                self.validation_results_directory,
            )
            get_io_file_manager().mkdir(self.validation_results_directory)

        get_io_file_manager().write(
            self.validation_results_file, self.validation_results.model_dump_json()
        )
        LOGGER.info(
            "Validation results successfully saved to: '%s'",
            self.validation_results_file,
        )

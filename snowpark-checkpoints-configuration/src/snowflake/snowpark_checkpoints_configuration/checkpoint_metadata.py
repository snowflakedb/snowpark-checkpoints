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

from snowflake.snowpark_checkpoints_configuration.io_utils.io_file_manager import (
    get_io_file_manager,
)
from snowflake.snowpark_checkpoints_configuration.model.checkpoints import (
    Checkpoint,
    Checkpoints,
)
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton


LOGGER = logging.getLogger(__name__)


class CheckpointMetadata(metaclass=Singleton):

    """CheckpointMetadata class.

    This is a singleton class that reads the checkpoints.json file
    and provides an interface to get the checkpoint configuration.

    Args:
        metaclass (Singleton, optional): Defaults to Singleton.

    """

    def __init__(self, path: Optional[str] = None):
        self.checkpoint_model: Checkpoints = Checkpoints(type="", pipelines=[])
        directory = path if path is not None else get_io_file_manager().getcwd()
        checkpoints_file = os.path.join(directory, "checkpoints.json")
        if get_io_file_manager().file_exists(checkpoints_file):
            LOGGER.info("Reading checkpoints file: '%s'", checkpoints_file)
            try:
                checkpoint_json = get_io_file_manager().read(checkpoints_file)
                self.checkpoint_model = Checkpoints.model_validate_json(checkpoint_json)
                LOGGER.info(
                    "Successfully read and validated checkpoints file: '%s'",
                    checkpoints_file,
                )
            except Exception as e:
                error_msg = f"An error occurred while reading the checkpoints file: '{checkpoints_file}'"
                LOGGER.exception(error_msg)
                raise Exception(f"{error_msg} \n {e}") from None
        else:
            LOGGER.warning("Checkpoints file not found: '%s'", checkpoints_file)

    def get_checkpoint(self, checkpoint_name: str) -> Checkpoint:
        """Get a checkpoint by its name.

        Args:
            checkpoint_name (str): checkpoint name

        Returns:
            Checkpoint: Checkpoint configuration instance

        """
        return self.checkpoint_model.get_check_point(checkpoint_name=checkpoint_name)

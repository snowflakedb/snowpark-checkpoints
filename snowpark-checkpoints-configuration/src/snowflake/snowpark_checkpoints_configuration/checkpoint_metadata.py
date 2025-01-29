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
import os

from snowflake.snowpark_checkpoints_configuration.model.checkpoints import (
    Checkpoint,
    Checkpoints,
)
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton


class CheckpointMetadata(metaclass=Singleton):

    """CheckpointMetadata class.

    This is a singleton class that reads the checkpoints.json file
    and provides an interface to get the checkpoint configuration.

    Args:
        metaclass (Singleton, optional): Defaults to Singleton.

    """

    def __init__(self, path: str = None):
        directory = path if path is not None else os.getcwd()
        self.checkpoint_model: Checkpoints = Checkpoints(type="", pipelines=[])
        checkpoints_file = os.path.join(directory, "checkpoints.json")
        if os.path.exists(checkpoints_file):
            with open(checkpoints_file) as f:
                try:
                    checkpoint_json = f.read()
                    self.checkpoint_model = Checkpoints.model_validate_json(
                        checkpoint_json
                    )
                except Exception as e:
                    raise Exception(
                        f"Error reading checkpoints file: {checkpoints_file} \n {e}"
                    ) from None

    def get_checkpoint(self, checkpoint_name: str) -> Checkpoint:
        """Get a checkpoint by its name.

        Args:
            checkpoint_name (str): checkpoint name

        Returns:
            Checkpoint: Checkpoint configuration instance

        """
        return self.checkpoint_model.get_check_point(checkpoint_name=checkpoint_name)

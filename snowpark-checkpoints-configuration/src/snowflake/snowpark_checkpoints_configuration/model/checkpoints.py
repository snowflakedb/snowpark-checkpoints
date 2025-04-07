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

from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic.alias_generators import to_camel

from snowflake.snowpark_checkpoints_configuration import checkpoint_name_utils


LOGGER = logging.getLogger(__name__)


class Checkpoint(BaseModel):

    """Checkpoint model.

    Args:
        pydantic.BaseModel (pydantic.BaseModel): pydantic BaseModel

    """

    name: str
    mode: int = 1
    function: Optional[str] = None
    df: Optional[str] = None
    sample: Optional[float] = None
    file: Optional[str] = None
    location: int = -1
    enabled: bool = True

    @field_validator("name", mode="before")
    @classmethod
    def normalize(cls, name: str) -> str:
        LOGGER.debug("Normalizing checkpoint name: '%s'", name)
        normalized_name = checkpoint_name_utils.normalize_checkpoint_name(name)
        LOGGER.debug("Checkpoint name was normalized to: '%s'", normalized_name)
        is_valid_checkpoint_name = checkpoint_name_utils.is_valid_checkpoint_name(
            normalized_name
        )
        if not is_valid_checkpoint_name:
            error_msg = (
                f"Invalid checkpoint name: {name} in checkpoints.json file. "
                f"Checkpoint names must only contain alphanumeric characters, underscores and dollar signs."
            )
            LOGGER.error(error_msg)
            raise Exception(error_msg)

        return normalized_name


class Pipeline(BaseModel):

    """Pipeline model.

    Args:
        pydantic.BaseModel (pydantic.BaseModel): pydantic BaseModel

    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )

    entry_point: str
    checkpoints: list[Checkpoint]


class Checkpoints(BaseModel):

    """Checkpoints model.

    Args:
        pydantic.BaseModel (pydantic.BaseModel): pydantic BaseModel

    Returns:
        Checkpoints: An instance of the Checkpoints class

    """

    type: str
    pipelines: list[Pipeline]

    # this dictionary holds the unpacked checkpoints from the different pipelines.
    _checkpoints = {}

    def _build_checkpoints_dict(self):
        LOGGER.debug("Building checkpoints dictionary from pipelines.")
        for pipeline in self.pipelines:
            LOGGER.debug("Processing pipeline: '%s'", pipeline.entry_point)
            for checkpoint in pipeline.checkpoints:
                LOGGER.debug("Adding checkpoint: %s", checkpoint)
                self._checkpoints[checkpoint.name] = checkpoint

    def get_check_point(self, checkpoint_name: str) -> Checkpoint:
        """Get a checkpoint by its name.

        Args:
            checkpoint_name (str): The name of the checkpoint.

        Returns:
            Checkpoint: The checkpoint object if found, otherwise a new Checkpoint object
            with the name set to the checkpoint_id.

        """
        LOGGER.info("Fetching checkpoint: '%s'", checkpoint_name)
        if not self._checkpoints:
            LOGGER.debug("Checkpoints dictionary is empty, building it...")
            self._build_checkpoints_dict()

        checkpoint = self._checkpoints.get(checkpoint_name)
        if len(self._checkpoints) == 0:
            LOGGER.info(
                "No checkpoints found, creating a new enabled checkpoint with name: '%s'",
                checkpoint_name,
            )
            checkpoint = Checkpoint(name=checkpoint_name, enabled=True)
        elif checkpoint is None:
            LOGGER.info(
                "Checkpoint not found, creating a new disabled checkpoint with name: '%s'",
                checkpoint_name,
            )
            checkpoint = Checkpoint(name=checkpoint_name, enabled=False)

        LOGGER.debug("Returning checkpoint: %s", checkpoint)
        return checkpoint

    def add_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Add a checkpoint to the checkpoints' dictionary.

        Args:
            checkpoint (Checkpoint): The checkpoint object to add

        """
        LOGGER.debug("Adding checkpoint: %s", checkpoint)
        self._checkpoints[checkpoint.name] = checkpoint
        LOGGER.info("Checkpoint '%s' added successfully.", checkpoint.name)

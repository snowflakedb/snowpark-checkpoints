#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic.alias_generators import to_camel

from snowflake.snowpark_checkpoints_configuration import checkpoint_name_utils


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
        normalized_name = checkpoint_name_utils.normalize_checkpoint_name(name)
        is_valid_checkpoint_name = checkpoint_name_utils.is_valid_checkpoint_name(
            normalized_name
        )
        if not is_valid_checkpoint_name:
            raise Exception(
                f"Invalid checkpoint name: {name}. Checkpoint names must only contain alphanumeric "
                f"characters and underscores."
            )

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
        for pipeline in self.pipelines:
            for checkpoint in pipeline.checkpoints:
                self._checkpoints[checkpoint.name] = checkpoint

    def get_check_point(self, checkpoint_name: str) -> Checkpoint:
        """Get a checkpoint by its name.

        Args:
            checkpoint_name (str): The name of the checkpoint.

        Returns:
            Checkpoint: The checkpoint object if found, otherwise a new Checkpoint object
            with the name set to the checkpoint_id.

        """
        if not self._checkpoints:
            self._build_checkpoints_dict()

        checkpoint = self._checkpoints.get(checkpoint_name)
        if len(self._checkpoints) == 0:
            checkpoint = Checkpoint(name=checkpoint_name, enabled=True)
        elif checkpoint is None:
            checkpoint = Checkpoint(name=checkpoint_name, enabled=False)
        return checkpoint

    def add_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Add a checkpoint to the checkpoints' dictionary.

        Args:.
            checkpoint (Checkpoint): The checkpoint object to add

        """
        self._checkpoints[checkpoint.name] = checkpoint

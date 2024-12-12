#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional


# noinspection DuplicatedCode
def _get_metadata():
    try:
        from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
            CheckpointMetadata,
        )

        metadata = CheckpointMetadata()
        return True, metadata

    except ImportError:
        return False, None


def is_checkpoint_enabled(checkpoint_name: Optional[str] = None) -> bool:
    """Check if a checkpoint is enabled.

    Args:
        checkpoint_name (Optional[str], optional): The name of the checkpoint.

    Returns:
        bool: True if the checkpoint is enabled, False otherwise.

    """
    enabled, metadata = _get_metadata()
    if enabled and checkpoint_name is not None:
        config = metadata.get_checkpoint(checkpoint_name)
        return config.enabled
    else:
        return True

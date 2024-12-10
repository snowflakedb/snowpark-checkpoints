#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

metadata = None
try:
    from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
        CheckpointMetadata,
    )

    configuration_enabled = True
    metadata = CheckpointMetadata()

except ImportError:
    configuration_enabled = False


def is_checkpoint_enabled(checkpoint_name: str) -> bool:
    """Check if a checkpoint is enabled.

    Args:
        checkpoint_name (str): The name of the checkpoint.

    Returns:
        bool: True if the checkpoint is enabled, False otherwise.

    """
    if configuration_enabled:
        config = metadata.get_checkpoint(checkpoint_name)
        return config.enabled
    else:
        return True

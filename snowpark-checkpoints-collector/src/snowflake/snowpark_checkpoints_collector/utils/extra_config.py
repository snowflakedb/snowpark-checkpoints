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


def get_checkpoint_sample(checkpoint_name: str, sample: float = None) -> float:
    """Get the checkpoint sample.

        Following this order first, the sample passed as argument, second, the sample from the checkpoint configuration,
        third, the default sample value 1.0.

    Args:
        checkpoint_name (str): The name of the checkpoint.
        sample (float, optional): The value passed to the function.

    Returns:
        float: returns the sample for that specific checkpoint.

    """
    default_sample = 1.0

    if configuration_enabled:
        config = metadata.get_checkpoint(checkpoint_name)
        default_sample = config.sample if config.sample is not None else 1.0

    return sample if sample is not None else default_sample

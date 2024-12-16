#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional

from snowflake.snowpark_checkpoints_collector.collection_common import CheckpointMode


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


def is_checkpoint_enabled(checkpoint_name: str) -> bool:
    """Check if a checkpoint is enabled.

    Args:
        checkpoint_name (str): The name of the checkpoint.

    Returns:
        bool: True if the checkpoint is enabled, False otherwise.

    """
    enabled, metadata = _get_metadata()
    if enabled:
        config = metadata.get_checkpoint(checkpoint_name)
        return config.enabled
    else:
        return True


def get_checkpoint_sample(
    checkpoint_name: str, sample: Optional[float] = None
) -> float:
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

    enabled, metadata = _get_metadata()
    if enabled:
        config = metadata.get_checkpoint(checkpoint_name)
        default_sample = config.sample if config.sample is not None else default_sample

    return sample if sample is not None else default_sample


def get_checkpoint_mode(
    checkpoint_name: str, mode: Optional[CheckpointMode] = None
) -> CheckpointMode:
    """Get the checkpoint mode.

        Following this order first, the mode passed as argument, second, the mode from the checkpoint configuration,
        third, the default mode value 1.

    Args:
        checkpoint_name (str): The name of the checkpoint.
        mode (int, optional): The value passed to the function.

    Returns:
        int: returns the mode for that specific checkpoint.

    """
    default_mode = CheckpointMode.SCHEMA

    enabled, metadata = _get_metadata()
    if enabled:
        config = metadata.get_checkpoint(checkpoint_name)
        default_mode = config.mode if config.mode is not None else default_mode

    return mode if mode is not None else default_mode


def get_checkpoint_location(checkpoint_name: str) -> int:
    """Get the checkpoint location.

    Following this order first, the mode from the checkpoint configuration, second, the default location value -1.

    Args:
        checkpoint_name (str): The name of the checkpoint.

    Returns:
        int: returns the location for that specific checkpoint.

    """
    default_location = -1
    enabled, metadata = _get_metadata()
    if enabled:
        config = metadata.get_checkpoint(checkpoint_name)
        location = config.location if config.location is not None else default_location
        return location

    return default_location


def get_checkpoint_df_name(checkpoint_name: str) -> str:
    """Get the checkpoint dataframe name.

    Following this order first, the mode from the checkpoint configuration, second,the default dataframe name value
    unknown.

    Args:
        checkpoint_name (str): The name of the checkpoint.

    Returns:
        str: returns the name of the dataframe for that specific checkpoint.

    """
    default_name = "unknown"
    enabled, metadata = _get_metadata()
    if enabled:
        config = metadata.get_checkpoint(checkpoint_name)
        df_name = config.df if config.df is not None else default_name
        return df_name

    return default_name

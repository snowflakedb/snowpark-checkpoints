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

from typing import Optional

from snowflake.snowpark_checkpoints_collector.collection_common import (
    SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR,
    CheckpointMode,
)


# noinspection DuplicatedCode
def _get_checkpoint_contract_file_path() -> str:
    return os.environ.get(SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR, os.getcwd())


# noinspection DuplicatedCode
def _get_metadata():
    try:
        from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
            CheckpointMetadata,
        )

        path = _get_checkpoint_contract_file_path()
        metadata = CheckpointMetadata(path)
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

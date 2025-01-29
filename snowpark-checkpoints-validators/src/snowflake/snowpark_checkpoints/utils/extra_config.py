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

from snowflake.snowpark_checkpoints.utils.constants import (
    SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR,
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


def get_checkpoint_file(checkpoint_name: str) -> Optional[str]:
    """Retrieve the configuration for a specified checkpoint.

    This function fetches the checkpoint configuration if metadata is enabled.
    It extracts the file name from the checkpoint metadata or
    from the call stack if not explicitly provided in the metadata.

    Args:
        checkpoint_name (str): The name of the checkpoint to retrieve the configuration for.

    Returns:
        Optional[dict]: A dictionary containing the file name,
                        or None if metadata is not enabled.

    """
    enabled, metadata = _get_metadata()
    if enabled:
        config = metadata.get_checkpoint(checkpoint_name)

        return config.file
    else:
        return None

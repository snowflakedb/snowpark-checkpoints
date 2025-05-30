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


# Add a NullHandler to prevent logging messages from being output to
# sys.stderr if no logging configuration is provided.
logging.getLogger(__name__).addHandler(logging.NullHandler())

# ruff: noqa: E402

from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
    CheckpointMetadata,
)


__all__ = [
    "CheckpointMetadata",
]

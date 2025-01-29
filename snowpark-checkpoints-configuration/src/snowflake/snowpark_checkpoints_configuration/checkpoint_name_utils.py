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

import re as regx


CHECKPOINT_NAME_REGEX_PATTERN = r"[a-zA-Z_][a-zA-Z0-9_]+"
TRANSLATION_TABLE = str.maketrans({" ": "_", "-": "_"})


def normalize_checkpoint_name(checkpoint_name: str) -> str:
    """Normalize the provided checkpoint name by replacing: the whitespace and hyphen tokens by underscore token.

    Args:
        checkpoint_name (str): The checkpoint name to normalize.

    Returns:
        str: the checkpoint name normalized.

    """
    normalized_checkpoint_name = checkpoint_name.translate(TRANSLATION_TABLE)
    return normalized_checkpoint_name


def is_valid_checkpoint_name(checkpoint_name: str) -> bool:
    """Check if the provided checkpoint name is valid.

    A valid checkpoint name must:
    - Start with a letter (a-z, A-Z) or an underscore (_)
    - Be followed by any combination of letters, digits (0-9) and underscores (_).

    Args:
        checkpoint_name (str): The checkpoint name to validate.

    Returns:
        bool: True if the checkpoint name is valid; otherwise, False.

    """
    matched = regx.fullmatch(CHECKPOINT_NAME_REGEX_PATTERN, checkpoint_name)
    is_valid = bool(matched)
    return is_valid

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re as regx


CHECKPOINT_NAME_REGEX_PATTERN = r"[a-zA-Z_][a-zA-Z0-9_]+"
WHITESPACE_TOKEN = " "
HYPHEN_TOKEN = "-"
UNDERSCORE_TOKEN = "_"
TOKEN_TO_REPLACE_COLLECTION = [WHITESPACE_TOKEN, HYPHEN_TOKEN]


def normalize_checkpoint_name(checkpoint_name: str) -> str:
    """Normalize the provided checkpoint name by replacing: the whitespace and hyphen tokens by underscore token.

    Args:
        checkpoint_name (str): The checkpoint name to normalize.

    Returns:
        str: the checkpoint name normalized.

    """
    normalized_checkpoint_name = checkpoint_name
    for token in TOKEN_TO_REPLACE_COLLECTION:
        normalized_checkpoint_name = normalized_checkpoint_name.replace(
            token, UNDERSCORE_TOKEN
        )

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

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from pathlib import Path
import re

VERSION_VARIABLE_PATTERN = r"^__version__ = ['\"]([^'\"]*)['\"]"
VERSION_FILE_NAME = "__version__.py"

def get_version() -> str:
    """Get the version of the package.
    Returns:
        str: The version of the package.
    """
    try:
        directory_levels_up = 3
        project_root = Path(__file__).resolve().parents[directory_levels_up]
        version_file_path = project_root / VERSION_FILE_NAME
        with open(version_file_path) as file:
            content = file.read()
            version_match = re.search(
                VERSION_VARIABLE_PATTERN, content, re.MULTILINE
            )
            if version_match:
                return version_match.group(1)
        return None
    except Exception:
        return None
    
get_version()
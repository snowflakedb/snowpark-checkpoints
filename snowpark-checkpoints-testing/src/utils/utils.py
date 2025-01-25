#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import re


def get_version() -> str:
    """Get the version of the package.
    Returns:
        str: The version of the package.
    """
    try:
        folder = os.path.abspath(os.path.join(__file__, "../../../../"))
        version_file_path = os.path.join(folder, "__version__.py")
        with open(version_file_path) as file:
            content = file.read()
            version_match = re.search(
                r"^__version__ = ['\"]([^'\"]*)['\"]", content, re.MULTILINE
            )
            if version_match:
                return version_match.group(1)
        return None
    except Exception:
        return None

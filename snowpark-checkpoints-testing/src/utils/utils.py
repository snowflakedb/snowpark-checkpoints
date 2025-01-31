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
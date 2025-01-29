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

import pytest

from snowflake.snowpark.session import Session


@pytest.fixture(scope="module")
def session() -> Session:
    """Define a fixture that returns a Snowpark session using the default connection.

    Returns:
        A Snowpark session with the default connection.

    """
    return Session.builder.getOrCreate()


@pytest.fixture(scope="module")
def local_session() -> Session:
    """Define a fixture that returns a Snowpark session with local_testing enabled.

    Returns:
        A Snowpark session with local_testing enabled.

    """
    return Session.builder.config("local_testing", True).getOrCreate()

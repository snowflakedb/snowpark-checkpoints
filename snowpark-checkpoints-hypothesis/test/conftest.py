#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

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

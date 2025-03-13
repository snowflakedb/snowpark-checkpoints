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

from unittest.mock import MagicMock

import pytest

from snowflake.snowpark_checkpoints import SnowparkJobContext


LOGGER_NAME = "snowflake.snowpark_checkpoints.job_context"


def test_mark_pass_log_results_disabled(caplog: pytest.LogCaptureFixture):
    """Test that pass results are not recorded into Snowflake when log_results is False."""
    caplog.set_level(level=logging.WARNING, logger=LOGGER_NAME)

    snowpark_session = MagicMock()
    job_context = SnowparkJobContext(snowpark_session, log_results=False)
    job_context._mark_pass(checkpoint_name="test-checkpoint")

    assert "Recording of migration results into Snowflake is disabled" in caplog.text


def test_mark_fail_log_results_disabled(caplog: pytest.LogCaptureFixture):
    """Test that fail results are not recorded into Snowflake when log_results is False."""
    caplog.set_level(level=logging.WARNING, logger=LOGGER_NAME)

    snowpark_session = MagicMock()
    job_context = SnowparkJobContext(snowpark_session, log_results=False)
    job_context._mark_fail(
        message="test-message", checkpoint_name="test-checkpoint", data="test-data"
    )

    assert "Recording of migration results into Snowflake is disabled" in caplog.text

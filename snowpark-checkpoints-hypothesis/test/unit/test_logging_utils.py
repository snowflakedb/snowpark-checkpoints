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

from functools import wraps

import pytest

from snowflake.hypothesis_snowpark.logging_utils import log


def test_log_decorator_logs_function_call_with_args(caplog: pytest.LogCaptureFixture):
    """Test that the log decorator logs the function call with the arguments."""

    @log
    def sample_function(a: int, b: int = 2):
        return a + b

    with caplog.at_level(logging.DEBUG):
        result = sample_function(1, b=3)

    assert result == 4
    assert "sample_function called with args 1, b=3" in caplog.text


def test_log_decorator_logs_function_call_without_args(
    caplog: pytest.LogCaptureFixture,
):
    """Test that the log decorator logs the function call without the arguments."""

    @log
    def sample_function():
        return 1

    with caplog.at_level(logging.DEBUG):
        result = sample_function()

    assert result == 1
    assert "sample_function called with args" in caplog.text


def test_log_decorator_logs_exceptions(caplog: pytest.LogCaptureFixture):
    """Test that the log decorator logs exceptions."""
    exception_msg = "An error occurred"

    @log
    def sample_function():
        raise ValueError(exception_msg)

    with pytest.raises(ValueError, match=exception_msg), caplog.at_level(logging.DEBUG):
        sample_function()

    assert "An error occurred in sample_function" in caplog.text
    assert exception_msg in caplog.text


def test_log_decorator_custom_logger(caplog: pytest.LogCaptureFixture):
    """Test that the log decorator logs to a custom logger."""
    logger_name = "custom_logger"
    custom_logger = logging.getLogger(logger_name)

    @log(logger=custom_logger)
    def sample_function(a: int, b: int):
        return a + b

    with caplog.at_level(logging.DEBUG, logger=logger_name):
        result = sample_function(1, 2)

    assert result == 3
    assert "sample_function called with args 1, 2" in caplog.text


def test_log_decorator_no_log_args(caplog: pytest.LogCaptureFixture):
    """Test that the log decorator does not log the arguments when log_args is False."""

    @log(log_args=False)
    def sample_function(a: int, b: int):
        return a + b

    with caplog.at_level(logging.DEBUG):
        result = sample_function(1, 2)

    assert result == 3
    assert "sample_function called with args" not in caplog.text


def test_log_decorator_with_other_decorators(caplog: pytest.LogCaptureFixture):
    """Test that the log decorator works with other decorators."""

    def dummy_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            raise ValueError("dummy_decorator called")

        return wrapper

    @dummy_decorator
    @log
    def sample_function_1():
        return 1

    @log
    @dummy_decorator
    def sample_function_2():
        return 2

    with caplog.at_level(logging.DEBUG), pytest.raises(
        ValueError, match="dummy_decorator called"
    ):
        sample_function_1()

    assert "An error occurred in sample_function" not in caplog.text

    with caplog.at_level(logging.DEBUG), pytest.raises(
        ValueError, match="dummy_decorator called"
    ):
        sample_function_2()

    assert "An error occurred in sample_function" in caplog.text

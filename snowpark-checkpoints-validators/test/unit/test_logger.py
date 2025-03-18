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

import io
import logging

from collections.abc import Generator
from importlib import import_module
from typing import Any, Union

import pytest


TOP_LEVEL_LOGGER_NAME = "snowflake.snowpark_checkpoints"

LoggerDict = dict[str, Union[logging.Logger, logging.PlaceHolder]]
LoggerGenerator = Generator[logging.Logger, Any, None]


@pytest.fixture(name="registered_loggers", scope="module")
def fixture_registered_loggers() -> LoggerDict:
    """Return a dictionary with all the registered loggers."""
    # We need to import the snowflake.snowpark_checkpoints module
    # to ensure the __init__.py file is executed.
    import_module("snowflake.snowpark_checkpoints")

    return logging.getLogger().manager.loggerDict


@pytest.fixture(name="top_level_logger", scope="module")
def fixture_top_level_logger(registered_loggers: LoggerDict) -> LoggerGenerator:
    """Return the top-level logger of the snowpark-checkpoints-validators package."""
    logger = registered_loggers.get(TOP_LEVEL_LOGGER_NAME, None)
    assert isinstance(logger, logging.Logger)

    # Save the original state of the logger
    original_handlers = logger.handlers
    original_propagate = logger.propagate
    original_level = logger.level
    original_filters = logger.filters

    yield logger

    # Restore the original state of the logger
    logger.handlers = original_handlers
    logger.propagate = original_propagate
    logger.setLevel(original_level)
    logger.filters = original_filters


def test_loggers_exists(registered_loggers: LoggerDict):
    """Validates that all the snowflake.snowpark_checkpoints loggers exist."""
    logger_names = {
        "snowflake.snowpark_checkpoints",
        "snowflake.snowpark_checkpoints.checkpoint",
        "snowflake.snowpark_checkpoints.job_context",
        "snowflake.snowpark_checkpoints.snowpark_sampler",
        "snowflake.snowpark_checkpoints.spark_migration",
        "snowflake.snowpark_checkpoints.utils.extra_config",
        "snowflake.snowpark_checkpoints.utils.pandera_check_manager",
        "snowflake.snowpark_checkpoints.utils.utils_checks",
        "snowflake.snowpark_checkpoints.validation_result_metadata",
    }

    for logger_name in logger_names:
        logger = registered_loggers.get(logger_name, None)
        assert logger is not None
        assert isinstance(logger, logging.Logger)


def test_top_level_logger_has_null_handler(top_level_logger: LoggerGenerator):
    """Validates that the top-level logger has a single handler and that it is a NullHandler."""
    assert isinstance(top_level_logger, logging.Logger)
    assert len(top_level_logger.handlers) == 1
    assert isinstance(top_level_logger.handlers[0], logging.NullHandler)


def test_top_level_logger_default_log_level(top_level_logger: LoggerGenerator):
    """Validates that the default log level of the top-level logger is logging.WARNING."""
    assert isinstance(top_level_logger, logging.Logger)
    assert top_level_logger.getEffectiveLevel() == logging.WARNING


def test_child_logger_inheritance(
    registered_loggers: LoggerDict, top_level_logger: LoggerGenerator
):
    """Validates the inheritance of the loggers."""
    child_logger = registered_loggers.get(
        f"{TOP_LEVEL_LOGGER_NAME}.spark_migration", None
    )
    assert isinstance(child_logger, logging.Logger)
    assert child_logger.parent == top_level_logger


def test_log_propagation(
    registered_loggers: LoggerDict, top_level_logger: LoggerGenerator
):
    """Validates that log messages are propagated from a child logger to the top-level logger."""
    assert isinstance(top_level_logger, logging.Logger)

    stream_handler = logging.StreamHandler(io.StringIO())
    top_level_logger.addHandler(stream_handler)

    child_logger = registered_loggers.get(
        f"{TOP_LEVEL_LOGGER_NAME}.spark_migration", None
    )
    assert isinstance(child_logger, logging.Logger)
    assert len(child_logger.handlers) == 0

    child_logger.warning("Test message")
    assert "Test message" in stream_handler.stream.getvalue()


def test_null_handler_supresses_output(
    capsys: pytest.CaptureFixture[str], top_level_logger: LoggerGenerator
):
    """Validates that NullHandler suppresses output to stderr."""
    assert isinstance(top_level_logger, logging.Logger)
    top_level_logger.propagate = False
    top_level_logger.error("This should not appear in stderr")
    captured = capsys.readouterr()
    assert captured.err == ""

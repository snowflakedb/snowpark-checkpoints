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

import os
import tempfile
from unittest.mock import patch
import pytest

from snowflake.snowpark_checkpoints_configuration.io_utils import IODefaultStrategy
from snowflake.snowpark_checkpoints_configuration.io_utils.io_file_manager import (
    get_io_file_manager,
)
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton


@pytest.fixture(scope="function")
def io_file_manager():
    Singleton._instances = {}
    return get_io_file_manager()


def test_default_file_exists(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_file.txt")
        with open(path, "w") as f:
            f.write("test")

        # Act
        result = io_file_manager.file_exists(path)

        # Assert
        assert result


def test_default_file_exists_fail(io_file_manager):
    # Act
    result_not_exists = io_file_manager.file_exists("")

    # Assert
    assert result_not_exists is False


def test_default_file_exists_fail_with_folder(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "temp_dir")
        os.makedirs(path)

        # Act
        result_not_file = io_file_manager.file_exists(path)

        # Assert
        assert result_not_file is False


def test_default_read(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_file.txt")
        content = "test"
        with open(path, "w") as f:
            f.write(content)

        # Act
        result = io_file_manager.read(path)

        # Assert
        assert result == content


def test_default_read_fail(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_file.txt")

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            io_file_manager.read(path)


def test_default_getcwd(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()):
        # Act
        result = io_file_manager.getcwd()

        # Assert
        assert isinstance(result, str)


def test_default_set_strategy(io_file_manager):
    try:
        # Arrange
        with patch(
            "snowflake.snowpark_checkpoints_configuration.io_utils.EnvStrategy"
        ) as MockStrategy:
            strategy = MockStrategy()

            # Act
            io_file_manager.set_strategy(strategy)

            # Assert
            assert io_file_manager.strategy == strategy
    finally:
        io_file_manager.set_strategy(IODefaultStrategy())


def test_default_get_io_file_manager_singleton():
    # Act
    result = get_io_file_manager()
    result2 = get_io_file_manager()

    # Assert
    assert result == result2

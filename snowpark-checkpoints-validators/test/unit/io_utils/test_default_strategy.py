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
from pathlib import Path
import tempfile
from unittest.mock import patch
import pytest

from snowflake.snowpark_checkpoints.io_utils import IODefaultStrategy
from snowflake.snowpark_checkpoints.io_utils.io_file_manager import (
    get_io_file_manager,
)
from snowflake.snowpark_checkpoints.singleton import Singleton


@pytest.fixture(scope="function")
def io_file_manager():
    Singleton._instances = {}
    return get_io_file_manager()


def test_default_mkdir(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_dir")

        # Act
        io_file_manager.mkdir(path)
        folder_exist = os.path.exists(path)

        # Assert
        assert folder_exist


def test_default_mkdir_fail(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_dir\0")

        # Act & Assert
        with pytest.raises(ValueError):
            io_file_manager.mkdir(path)


def test_default_folder_exists(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir)

        # Act
        result = io_file_manager.folder_exists(path)

        # Assert
        assert result


def test_default_folder_exists_fail(io_file_manager):
    # Act
    result_not_exist = io_file_manager.folder_exists("")

    # Assert
    assert result_not_exist is False


def test_default_folder_exists_fail_with_file(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_file.txt")
        with open(path, "w") as f:
            f.write("test")

        # Act
        result_not_folder = io_file_manager.folder_exists(path)

        # Assert
        assert result_not_folder is False


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


def test_default_write(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_file.txt")
        content = "test"

        # Act
        io_file_manager.write(path, content)
        with open(path) as f:
            file_content = f.read()

        # Assert
        assert content == file_content


def test_default_write_fail(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        content = "test"

        # Act & Assert
        with pytest.raises(OSError):
            io_file_manager.write(temp_dir, content)


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


def test_default_read_bytes(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_file.txt")
        content = "test"
        with open(path, "wb") as f:
            f.write(content.encode())

        # Act
        result = io_file_manager.read_bytes(path)

        # Assert
        assert result.decode() == content


def test_default_read_bytes_fail(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_file.txt")

        # Act & Assert
        with pytest.raises(FileNotFoundError):
            io_file_manager.read_bytes(path)


def test_default_ls(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_dir")
        os.makedirs(path)

        # Act
        result = io_file_manager.ls(temp_dir + "/*")

        # Assert
        assert len(result) == 1
        assert result[0] == path


def test_default_ls_case_2(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        file1 = os.path.join(temp_dir, "test.txt")
        file2 = os.path.join(temp_dir, "test2.py")

        with open(file1, "w") as f:
            f.write("test")

        with open(file2, "w") as f:
            f.write("test")

        # Act
        result = io_file_manager.ls(temp_dir + "/*.txt")

        # Assert
        assert len(result) == 1
        assert result[0] == file1


def test_default_ls_fail(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()):
        # Arrange
        path = None

        # Act
        result = io_file_manager.ls("")

        # Assert
        assert len(result) == 0


def test_default_getcwd(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()):
        # Act
        result = io_file_manager.getcwd()

        # Assert
        assert isinstance(result, str)


def test_default_telemetry_path_files(io_file_manager):
    with tempfile.TemporaryDirectory(dir=os.getcwd()) as temp_dir:
        # Arrange
        path = os.path.join(temp_dir, "test_dir")

        # Act
        result = io_file_manager.telemetry_path_files(path)

        # Assert
        assert result == Path(path)


def test_default_set_strategy(io_file_manager):
    try:
        # Arrange
        with patch(
            "snowflake.snowpark_checkpoints.io_utils.EnvStrategy"
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

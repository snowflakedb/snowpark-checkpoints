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
import os.path
import tempfile

import pytest

from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
    CheckpointMetadata,
)
from snowflake.snowpark_checkpoints_configuration.model.checkpoints import (
    Checkpoint,
    Checkpoints,
    Pipeline,
)
from snowflake.snowpark_checkpoints_configuration.singleton import Singleton


@pytest.fixture(autouse=True)
def singleton():
    Singleton._instances = {}


LOGGER_NAME = "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata"


def test_checkpoint_metadata_loading(caplog: pytest.LogCaptureFixture):
    caplog.set_level(level=logging.INFO, logger=LOGGER_NAME)
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, "valid_checkpoint")
    metadata = CheckpointMetadata(path)
    expected_checkpoint_1 = Checkpoint(
        name="demo_initial_creation_checkpoint",
        df="df",
        mode=1,
        function="",
        file="demo_pyspark_pipeline.py",
        location=1,
        enabled=True,
    )
    expected_checkpoint_2 = Checkpoint(
        name="demo_pyspark_pipeline_df1_123HDK",
        df="df1",
        mode=1,
        sample=0.5,
        file="demo_pyspark_pipeline.py",
        location=1,
        enabled=True,
    )
    expected_pipeline = Pipeline(
        entry_point="demo_pyspark_pipeline.py",
        checkpoints=[expected_checkpoint_1, expected_checkpoint_2],
    )

    expected_checkpoints = Checkpoints(type="Collection", pipelines=[expected_pipeline])
    assert metadata.checkpoint_model == expected_checkpoints
    assert "Successfully read and validated checkpoints file" in caplog.text


def test_checkpoint_metadata_loading_no_file(caplog: pytest.LogCaptureFixture):
    caplog.set_level(level=logging.WARNING, logger=LOGGER_NAME)
    path = tempfile.gettempdir()
    metadata = CheckpointMetadata(path)
    expected_checkpoints = Checkpoints(type="", pipelines=[])
    assert metadata.checkpoint_model == expected_checkpoints
    assert "Checkpoints file not found" in caplog.text


def test_checkpoint_metadata_loading_invalid_file(caplog: pytest.LogCaptureFixture):
    caplog.set_level(level=logging.ERROR, logger=LOGGER_NAME)
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, "invalid_checkpoint")
    checkpoint_file_name = os.path.join(path, "checkpoints.json")
    expected_error_msg = f"An error occurred while reading the checkpoints file: '{checkpoint_file_name}'"
    with pytest.raises(Exception) as ex_info:
        CheckpointMetadata(path)
    assert str(ex_info.value).startswith(expected_error_msg)
    assert expected_error_msg in caplog.text
    assert any(
        record.exc_info is not None for record in caplog.records
    ), "Log message does not contain the exception details, but it should"


def test_checkpoint_metadata_get_checkpoint_exist():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, "valid_checkpoint")
    metadata = CheckpointMetadata(path)
    checkpoint = metadata.get_checkpoint("demo_initial_creation_checkpoint")
    expected_checkpoint = Checkpoint(
        name="demo_initial_creation_checkpoint",
        df="df",
        mode=1,
        function="",
        file="demo_pyspark_pipeline.py",
        location=1,
        enabled=True,
    )
    assert checkpoint == expected_checkpoint


def test_checkpoint_metadata_get_checkpoint_not_exist():
    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, "valid_checkpoint")
    metadata = CheckpointMetadata(path)
    checkpoint = metadata.get_checkpoint("not-exist-checkpoint")
    expected_checkpoint = Checkpoint(name="not-exist-checkpoint", enabled=False)
    assert checkpoint == expected_checkpoint


def test_checkpoint_metadata_get_checkpoint_no_file():
    path = tempfile.gettempdir()
    metadata = CheckpointMetadata(path)
    checkpoint = metadata.get_checkpoint("not-exist-checkpoint")
    expected_checkpoint = Checkpoint(name="not-exist-checkpoint", enabled=True)
    assert checkpoint == expected_checkpoint

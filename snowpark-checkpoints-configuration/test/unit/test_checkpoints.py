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
import pytest

from snowflake.snowpark_checkpoints_configuration.model.checkpoints import (
    Checkpoint,
    Checkpoints,
    Pipeline,
)


LOGGER_NAME = "snowflake.snowpark_checkpoints_configuration.model.checkpoints"


def test_add_checkpoint(caplog: pytest.LogCaptureFixture):
    caplog.set_level(level=logging.DEBUG, logger=LOGGER_NAME)
    checkpoints = Checkpoints(type="Collection", pipelines=[])
    normalized_checkpoint_name = "checkpoint_name"
    new_checkpoint = Checkpoint(
        name="checkpoint-name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )
    checkpoints.add_checkpoint(new_checkpoint)
    assert checkpoints.get_check_point(normalized_checkpoint_name) == new_checkpoint
    assert (
        f"Checkpoint '{normalized_checkpoint_name}' added successfully" in caplog.text
    )


def test_add_checkpoint_with_same_name():
    checkpoints = Checkpoints(type="Collection", pipelines=[])
    new_checkpoint = Checkpoint(
        name="checkpoint-name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )
    checkpoints.add_checkpoint(new_checkpoint)
    new_checkpoint_2 = Checkpoint(
        name="checkpoint_name",
        df="df2",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=True,
    )
    checkpoints.add_checkpoint(new_checkpoint_2)

    assert checkpoints.get_check_point("checkpoint_name") == new_checkpoint_2


def test_get_checkpoint_existing():
    new_checkpoint = Checkpoint(
        name="checkpoint-name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )
    new_pipeline = Pipeline(entry_point="entry-point", checkpoints=[new_checkpoint])
    checkpoints = Checkpoints(type="Collection", pipelines=[new_pipeline])

    expected_checkpoint = Checkpoint(
        name="checkpoint_name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )

    assert checkpoints.get_check_point("checkpoint_name") == expected_checkpoint


def test_get_checkpoint_non_existing_empty_dict(caplog: pytest.LogCaptureFixture):
    caplog.set_level(level=logging.DEBUG, logger=LOGGER_NAME)
    checkpoints = Checkpoints(type="Collection", pipelines=[])
    checkpoint_name = "checkpoint-name-2"
    checkpoint = checkpoints.get_check_point(checkpoint_name)
    assert checkpoint == Checkpoint(name=checkpoint_name, enabled=True)
    assert "creating a new enabled checkpoint" in caplog.text


def test_get_checkpoint_no_existing_non_empty_dict(caplog: pytest.LogCaptureFixture):
    caplog.set_level(level=logging.INFO, logger=LOGGER_NAME)
    checkpoint = Checkpoint(
        name="checkpoint_name_1",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )
    pipeline = Pipeline(entry_point="entry-point", checkpoints=[checkpoint])
    checkpoints = Checkpoints(type="Collection", pipelines=[pipeline])
    checkpoint_name = "checkpoint_name_2"
    checkpoint = checkpoints.get_check_point(checkpoint_name)

    assert checkpoint == Checkpoint(name=checkpoint_name, enabled=False)
    assert "creating a new disabled checkpoint" in caplog.text

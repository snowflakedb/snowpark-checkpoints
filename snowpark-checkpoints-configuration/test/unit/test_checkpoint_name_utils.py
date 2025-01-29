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

from snowflake.snowpark_checkpoints_configuration.model.checkpoints import (
    Checkpoint,
)


@pytest.mark.parametrize(
    "input_value,expected_value",
    [("my checkpoint", "my_checkpoint"), ("my  checkpoint ", "my__checkpoint_")],
)
def test_normalize_checkpoint_name_whitespace_case(input_value, expected_value):
    checkpoint = Checkpoint(
        name=input_value,
        df="df",
        mode=1,
        function="",
        file="demo_pyspark_pipeline.py",
        location=1,
        enabled=True,
    )
    assert checkpoint.name == expected_value


@pytest.mark.parametrize(
    "input_value,expected_value",
    [
        ("my-checkpoint", "my_checkpoint"),
    ],
)
def test_normalize_checkpoint_name_hyphen_case(input_value, expected_value):
    checkpoint = Checkpoint(
        name=input_value,
        df="df",
        mode=1,
        function="",
        file="demo_pyspark_pipeline.py",
        location=1,
        enabled=True,
    )

    assert checkpoint.name == expected_value


@pytest.mark.parametrize(
    "input_value", ["_checkpoint1", "_checkpoint", "checkPoint1", "Checkpoint", "_1"]
)
def test_validate_checkpoint_name_valid_case(input_value):
    checkpoint = Checkpoint(
        name=input_value,
        df="df",
        mode=1,
        function="",
        file="demo_pyspark_pipeline.py",
        location=1,
        enabled=True,
    )

    assert checkpoint.name == input_value


@pytest.mark.parametrize(
    "input_value", ["_", "5", "", "56_my_checkpoint", "my-check", "_+check"]
)
def test_checkpoint_invalid_name(input_value):
    with pytest.raises(Exception) as ex_info:
        Checkpoint(
            name=input_value,
            df="df",
            mode=1,
            function="",
            file="demo_pyspark_pipeline.py",
            location=1,
            enabled=True,
        )
        assert (
            f"Invalid checkpoint name: {Checkpoint.name}. Checkpoint names must only contain alphanumeric "
            f"characters and underscores."
        ) == str(ex_info.value)

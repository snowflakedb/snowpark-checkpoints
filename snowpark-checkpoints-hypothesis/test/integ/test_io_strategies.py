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
import inspect
from pathlib import Path
import tempfile

from unittest.mock import patch

import hypothesis.strategies as st
import pandera as pa
import pytest

from hypothesis import HealthCheck, given, settings

from snowflake.hypothesis_snowpark import dataframe_strategy
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark.types import (
    ArrayType,
    LongType,
    StructField,
    StructType,
)
from snowflake.hypothesis_snowpark.io_utils.io_file_manager import IODefaultStrategy
from snowflake.hypothesis_snowpark.io_utils.io_file_manager import (
    get_io_file_manager,
)
from .telemetry_compare_utils import (
    validate_telemetry_file_output,
    reset_telemetry_util,
)

SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME = "snowpark-checkpoints-output"

TELEMETRY_FOLDER = "telemetry"


@pytest.fixture(scope="function")
def telemetry_output_path():
    folder = os.urandom(8).hex()
    directory = Path(tempfile.gettempdir()).resolve() / folder
    os.makedirs(directory)
    telemetry_dir = directory / TELEMETRY_FOLDER
    return telemetry_dir


@given(data=st.data())
@settings(deadline=None, max_examples=1, suppress_health_check=list(HealthCheck))
def test_io_strategy_from_json(
    data: st.DataObject, session: Session, telemetry_output_path: Path
):
    try:
        # Arrange
        class TestStrategy(IODefaultStrategy):
            pass

        number_of_methods = inspect.getmembers(
            IODefaultStrategy, predicate=inspect.isfunction
        )
        strategy = TestStrategy()
        get_io_file_manager().set_strategy(strategy)

        with (
            patch.object(strategy, "getcwd", wraps=strategy.getcwd) as getcwd_spy,
            patch.object(strategy, "ls", wraps=strategy.ls) as ls_spy,
            patch.object(strategy, "mkdir", wraps=strategy.mkdir) as mkdir_spy,
            patch.object(strategy, "write", wraps=strategy.write) as write_spy,
            patch.object(strategy, "read", wraps=strategy.read) as read_spy,
            patch.object(
                strategy, "read_bytes", wraps=strategy.read_bytes
            ) as read_bytes_spy,
            patch.object(
                strategy, "file_exists", wraps=strategy.file_exists
            ) as file_exists_spy,
            patch.object(
                strategy, "folder_exists", wraps=strategy.folder_exists
            ) as folder_exists_spy,
        ):
            telemetry_manager = reset_telemetry_util()
            telemetry_manager.set_sc_output_path(telemetry_output_path)
            # Act
            strategy = dataframe_strategy(
                schema="test/resources/test_io.json",
                session=session,
            )
            df = data.draw(strategy)

            expected_schema = StructType(
                [
                    StructField("array_column", ArrayType(), True),
                ]
            )

            # Assert
            assert df.schema == expected_schema
            assert isinstance(df, DataFrame)
            assert len(number_of_methods) == 9
            read_bytes_spy.assert_not_called()
            folder_exists_spy.assert_not_called()

            assert file_exists_spy.call_count == 1
            assert read_spy.call_count == 3
            assert write_spy.call_count == 1
            assert mkdir_spy.call_count == 3
            assert getcwd_spy.call_count == 1
            assert ls_spy.call_count == 1
            validate_telemetry_file_output(
                "test_io_strategy_from_json_telemetry.json", str(telemetry_output_path)
            )
    finally:
        get_io_file_manager().set_strategy(IODefaultStrategy())


@given(data=st.data())
@settings(deadline=None, max_examples=1, suppress_health_check=list(HealthCheck))
def test_io_strategy_from_object(
    data: st.DataObject, session: Session, telemetry_output_path: Path
):
    try:
        # Arrange
        class TestStrategy(IODefaultStrategy):
            pass

        number_of_methods = inspect.getmembers(
            IODefaultStrategy, predicate=inspect.isfunction
        )
        strategy = TestStrategy()
        get_io_file_manager().set_strategy(strategy)

        with (
            patch.object(strategy, "getcwd", wraps=strategy.getcwd) as getcwd_spy,
            patch.object(strategy, "ls", wraps=strategy.ls) as ls_spy,
            patch.object(strategy, "mkdir", wraps=strategy.mkdir) as mkdir_spy,
            patch.object(strategy, "write", wraps=strategy.write) as write_spy,
            patch.object(strategy, "read", wraps=strategy.read) as read_spy,
            patch.object(
                strategy, "read_bytes", wraps=strategy.read_bytes
            ) as read_bytes_spy,
            patch.object(
                strategy, "file_exists", wraps=strategy.file_exists
            ) as file_exists_spy,
            patch.object(
                strategy, "folder_exists", wraps=strategy.folder_exists
            ) as folder_exists_spy,
        ):
            telemetry_manager = reset_telemetry_util()
            telemetry_manager.set_sc_output_path(telemetry_output_path)
            schema = pa.DataFrameSchema(
                {"short_column": pa.Column(pa.Int16, nullable=True)}
            )

            # Act
            strategy = dataframe_strategy(
                schema=schema,
                session=session,
            )
            df = data.draw(strategy)

            expected_schema = StructType(
                [StructField("short_column", LongType(), True)]
            )

            # Assert
            assert df.schema == expected_schema
            assert isinstance(df, DataFrame)
            assert len(number_of_methods) == 9
            file_exists_spy.assert_not_called()
            read_bytes_spy.assert_not_called()
            folder_exists_spy.assert_not_called()

            assert read_spy.call_count == 1
            assert write_spy.call_count == 1
            assert mkdir_spy.call_count == 3
            assert getcwd_spy.call_count == 1
            assert ls_spy.call_count == 1
            validate_telemetry_file_output(
                "test_io_strategy_from_object_telemetry.json",
                str(telemetry_output_path),
            )
    finally:
        get_io_file_manager().set_strategy(IODefaultStrategy())

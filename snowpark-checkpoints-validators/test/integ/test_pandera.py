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

import json
import os
from unittest.mock import patch
from numpy import int8
from pandas import DataFrame as PandasDataFrame
from pandera import DataFrameSchema, Column, Check
from pytest import raises
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame

from snowflake.snowpark_checkpoints.checkpoint import (
    _check_dataframe_schema_file,
    check_dataframe_schema,
    check_output_schema,
    check_input_schema,
)
from snowflake.snowpark import Session

from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.utils.constants import (
    CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME,
    FAIL_STATUS,
    PASS_STATUS,
    SKIP_ALL,
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
)
import pytest
import os
from pathlib import Path
import tempfile
from snowflake.snowpark_checkpoints.utils.telemetry import (
    get_telemetry_manager,
)
from telemetry_compare_utils import validate_telemetry_file_output

TELEMETRY_FOLDER = "telemetry"


@pytest.fixture(scope="function")
def telemetry_output_path():
    folder = os.urandom(8).hex()
    directory = Path(tempfile.gettempdir()).resolve() / folder
    os.makedirs(directory)
    telemetry_dir = directory / TELEMETRY_FOLDER

    telemetry_manager = get_telemetry_manager()
    telemetry_manager.set_sc_output_path(telemetry_dir)
    return str(telemetry_dir)


def test_input(telemetry_output_path):
    checkpoint_name = "test_checkpoint"
    output__path = "test_output_path/unit/"
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    in_schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2, element_wise=True)),
        }
    )

    @check_input_schema(in_schema, checkpoint_name, output_path=output__path)
    def preprocessor(dataframe: SnowparkDataFrame):
        dataframe = dataframe.withColumn(
            "COLUMN3", dataframe["COLUMN1"] + dataframe["COLUMN2"]
        )
        return dataframe

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    with patch(
        "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
    ) as mock_update_validation_result:
        preprocessor(sp_df)

    mock_update_validation_result.assert_called_once_with(
        checkpoint_name, PASS_STATUS, output__path
    )
    validate_telemetry_file_output("test_input_telemetry.json", telemetry_output_path)


def test_input_fail(telemetry_output_path):
    checkpoint_name = "test_checkpoint"
    output_path = "test_output_path/unit/"
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 19, 0, 10, 7],
            "COLUMN2": [-1.3, -1.4, -3.9, -1.1, -20.4],
        }
    )

    in_schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 5, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2, element_wise=True)),
        }
    )

    @check_input_schema(in_schema, checkpoint_name, output_path=output_path)
    def preprocessor(dataframe: SnowparkDataFrame):
        dataframe = dataframe.withColumn(
            "COLUMN3", dataframe["COLUMN1"] + dataframe["COLUMN2"]
        )
        return dataframe

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    with (
        patch(
            "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
        ) as mock_update_validation_result,
        raises(SchemaValidationError),
    ):
        preprocessor(sp_df)

    mock_update_validation_result.assert_called_once_with(
        checkpoint_name, FAIL_STATUS, output_path
    )
    validate_telemetry_file_output(
        "test_input_fail_telemetry.json", telemetry_output_path
    )


def test_output(telemetry_output_path):
    checkpoint_name = "test_checkpoint"
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    out_schema = DataFrameSchema(
        {
            "COLUMN1": Column(
                int8, Check.between(0, 10, include_max=True, include_min=True)
            ),
            "COLUMN2": Column(float, Check.less_than_or_equal_to(-1.2)),
            "COLUMN3": Column(float, Check.less_than(10)),
        }
    )

    @check_output_schema(out_schema, checkpoint_name)
    def preprocessor(dataframe: SnowparkDataFrame):
        return dataframe.with_column(
            "COLUMN3", dataframe["COLUMN1"] + dataframe["COLUMN2"]
        )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    with patch(
        "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
    ) as mock_update_validation_result:
        preprocessor(sp_df)

    mock_update_validation_result.assert_called_once_with(
        checkpoint_name, PASS_STATUS, None
    )
    validate_telemetry_file_output("test_output_telemetry.json", telemetry_output_path)


def test_output_fail(telemetry_output_path):
    checkpoint_name = "test_checkpoint"
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 7, 0, 11, 9],
            "COLUMN2": [-1.3, -1.6, -2.9, -5.1, -10.4],
        }
    )

    out_schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check.between(0, 10)),
            "COLUMN2": Column(float, Check.less_than_or_equal_to(-1.2)),
            "COLUMN3": Column(float, Check.less_than(10)),
        }
    )

    @check_output_schema(out_schema, checkpoint_name)
    def preprocessor(dataframe: SnowparkDataFrame):
        return dataframe.with_column(
            "COLUMN3", dataframe["COLUMN1"] + dataframe["COLUMN2"]
        )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    with (
        patch(
            "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
        ) as mock_update_validation_result,
        raises(SchemaValidationError),
    ):
        preprocessor(sp_df)

    mock_update_validation_result.assert_called_once_with(
        checkpoint_name, FAIL_STATUS, None
    )
    validate_telemetry_file_output(
        "test_output_fail_telemetry.json", telemetry_output_path
    )


def test_df_check(telemetry_output_path):
    checkpoint_name = "test_checkpoint"
    output_path = "test_output_path/unit/"
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2, element_wise=True)),
        }
    )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    with (
        patch(
            "snowflake.snowpark_checkpoints.checkpoint.is_checkpoint_enabled",
            return_value=True,
        ),
        patch(
            "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
        ) as mocked_update,
    ):
        check_dataframe_schema(sp_df, schema, checkpoint_name, output_path=output_path)

    mocked_update.assert_called_once_with(checkpoint_name, PASS_STATUS, output_path)
    validate_telemetry_file_output(
        "test_df_check_telemetry.json", telemetry_output_path
    )


def test_df_check_fail(telemetry_output_path):
    checkpoint_name = "test_checkpoint"
    output_path = "test_output_path/unit/"
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 5, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2, element_wise=True)),
        }
    )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    with (
        patch(
            "snowflake.snowpark_checkpoints.checkpoint.is_checkpoint_enabled",
            return_value=True,
        ),
        patch(
            "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
        ) as mocked_update,
        raises(SchemaValidationError),
    ):
        check_dataframe_schema(sp_df, schema, checkpoint_name, output_path=output_path)

    mocked_update.assert_called_once_with(checkpoint_name, FAIL_STATUS, output_path)
    validate_telemetry_file_output(
        "test_df_check_fail_telemetry.json", telemetry_output_path
    )


def test_df_check_from_file(telemetry_output_path):
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
            "COLUMN3": [True, False, True, False, True],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check.between(0, 10)),
            "COLUMN2": Column(float, Check.between(-20.5, -1.0)),
        }
    )

    schema_data = {
        "pandera_schema": json.loads(schema.to_json()),
        "custom_data": {
            "columns": [
                {
                    "name": "COLUMN1",
                    "type": "integer",
                    "rows_count": 5,
                    "rows_not_null_count": 5,
                    "rows_null_count": 0,
                    "min": 0,
                    "max": 10,
                    "mean": 4.8,
                    "decimal_precision": 0,
                    "margin_error": 4.0693979898752,
                },
                {
                    "name": "COLUMN2",
                    "type": "float",
                    "rows_count": 5,
                    "rows_not_null_count": 5,
                    "rows_null_count": 0,
                    "min": -20.4,
                    "max": -1.3,
                    "mean": -7.22,
                    "decimal_precision": 1,
                    "margin_error": 7.3428604780426,
                },
            ],
        },
    }

    checkpoint_name = "test_checkpoint"

    current_directory_path = os.getcwd()

    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )

    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path)

    checkpoint_file_name = CHECKPOINT_JSON_OUTPUT_FILE_FORMAT_NAME.format(
        checkpoint_name
    )

    checkpoint_file_path = os.path.join(output_directory_path, checkpoint_file_name)

    with open(checkpoint_file_path, "w") as output_file:
        output_file.write(json.dumps(schema_data))

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)

    with (
        patch(
            "snowflake.snowpark_checkpoints.checkpoint.is_checkpoint_enabled",
            return_value=True,
        ),
        patch(
            "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
        ) as mocked_update,
    ):
        _check_dataframe_schema_file(sp_df, checkpoint_name)

    mocked_update.assert_called_once_with(checkpoint_name, PASS_STATUS, None)
    validate_telemetry_file_output(
        "test_df_check_from_file_telemetry.json", telemetry_output_path
    )


def test_df_check_custom_check(telemetry_output_path):
    checkpoint_name = "test_checkpoint"

    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check(lambda x: 0 <= x <= 10, element_wise=True)),
            "COLUMN2": Column(float, Check(lambda x: x < -1.2, element_wise=True)),
        }
    )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)
    with (
        patch(
            "snowflake.snowpark_checkpoints.checkpoint.is_checkpoint_enabled",
            return_value=True,
        ),
        patch(
            "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
        ) as mocked_update,
    ):
        check_dataframe_schema(
            sp_df,
            schema,
            checkpoint_name,
            custom_checks={
                "COLUMN1": [Check(lambda x: x.shape[0] == 5)],
                "COLUMN2": [Check(lambda x: x.shape[0] == 5)],
            },
        )

    mocked_update.assert_called_once_with(checkpoint_name, PASS_STATUS, None)
    assert len(schema.columns["COLUMN1"].checks) == 2
    assert len(schema.columns["COLUMN2"].checks) == 2
    validate_telemetry_file_output(
        "test_df_check_custom_check_telemetry.json", telemetry_output_path
    )


def test_df_check_skip_check(telemetry_output_path):
    checkpoint_name = "test_checkpoint"
    df = PandasDataFrame(
        {
            "COLUMN1": [1, 4, 0, 10, 9],
            "COLUMN2": [-1.3, -1.4, -2.9, -10.1, -20.4],
        }
    )

    schema = DataFrameSchema(
        {
            "COLUMN1": Column(int8, Check.between(0, 10, element_wise=True)),
            "COLUMN2": Column(
                float,
                [
                    Check.greater_than(-20.5),
                    Check.less_than(-1.0),
                    Check(lambda x: x < -1.2),
                ],
            ),
        }
    )

    session = Session.builder.getOrCreate()
    sp_df = session.create_dataframe(df)
    with (
        patch(
            "snowflake.snowpark_checkpoints.checkpoint.is_checkpoint_enabled",
            return_value=True,
        ),
        patch(
            "snowflake.snowpark_checkpoints.checkpoint._update_validation_result"
        ) as mocked_update,
    ):
        check_dataframe_schema(
            sp_df,
            schema,
            checkpoint_name,
            skip_checks={
                "COLUMN1": [SKIP_ALL],
                "COLUMN2": ["greater_than", "less_than"],
            },
        )

    mocked_update.assert_called_once_with(checkpoint_name, PASS_STATUS, None)
    assert len(schema.columns["COLUMN1"].checks) == 0
    assert len(schema.columns["COLUMN2"].checks) == 1
    validate_telemetry_file_output(
        "test_df_check_skip_check_telemetry.json", telemetry_output_path
    )

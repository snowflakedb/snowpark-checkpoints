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

from typing import get_type_hints
from unittest.mock import patch

from snowflake.snowpark_checkpoints.checkpoint import (
    validate_dataframe_checkpoint,
    xvalidate_dataframe_checkpoint,
)
from snowflake.snowpark_checkpoints.utils.constants import (
    SKIP_STATUS,
)
from unittest.mock import MagicMock
from snowflake.snowpark_checkpoints.validation_result_metadata import (
    ValidationResultsMetadata,
)


def test_xvalidate_dataframe_checkpoint():
    checkpoint_name = "my_checkpoint"
    module_name = "snowflake.snowpark_checkpoints.validation_result_metadata"
    validation_results_metadata = ValidationResultsMetadata("some/dummy/path")
    with (
        patch(
            f"{module_name}.ValidationResultsMetadata",
            return_value=validation_results_metadata,
        )
    ):
        pyspark_df = MagicMock()
        checkpoint_name = "my_checkpoint"

        xvalidate_dataframe_checkpoint(pyspark_df, checkpoint_name)

        validation_results = validation_results_metadata.validation_results
        validation_results = validation_results.model_dump()
        my_checkpoint_result = validation_results["results"][0]

        assert my_checkpoint_result["checkpoint_name"] == checkpoint_name
        assert my_checkpoint_result["result"] == SKIP_STATUS


def test_skip_validator_parameters_commutability():
    validation_hints = get_type_hints(validate_dataframe_checkpoint)
    x_validation_hints = get_type_hints(xvalidate_dataframe_checkpoint)

    validate_params = {
        name: hint for name, hint in validation_hints.items() if name != "return"
    }
    x_validate_params = {
        name: hint for name, hint in x_validation_hints.items() if name != "return"
    }
    assert (
        validate_params == x_validate_params
    ), "The parameters of validate_dataframe_checkpoint and xvalidate_dataframe_checkpoint must be the same."


def test_skip_validator_return_type_commutability():
    validation_hints = get_type_hints(validate_dataframe_checkpoint)
    x_validation_hints = get_type_hints(xvalidate_dataframe_checkpoint)

    validate_return = validation_hints.get("return")
    x_validate_return = x_validation_hints.get("return")
    assert (
        validate_return == x_validate_return
    ), "The return type of validate_dataframe_checkpoint and xvalidate_dataframe_checkpoint must be the same."

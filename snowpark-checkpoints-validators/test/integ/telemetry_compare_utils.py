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
import json
from deepdiff import DeepDiff

TEST_TELEMETRY_VALIDATORS_EXPECTED_DIRECTORY_NAME = "telemetry_expected"


def get_output_telemetry(telemetry_directory_path: str) -> str:
    for file in os.listdir(telemetry_directory_path):
        if file.endswith(".json"):
            output_file_path = os.path.join(telemetry_directory_path, file)
            with open(output_file_path) as f:
                return f.read().strip()
    return "{}"


def validate_telemetry_file_output(
    telemetry_file_name: str, telemetry_directory_path: str
) -> None:
    telemetry_expected = get_expected(telemetry_file_name)
    telemetry_output = get_output_telemetry(telemetry_directory_path)

    telemetry_expected_obj = json.loads(telemetry_expected)
    telemetry_output_obj = json.loads(telemetry_output)
    exclude_telemetry_paths = [
        "root['timestamp']",
        "root['message']['metadata']",
        "root['message']['driver_version']",
    ]

    diff_telemetry = DeepDiff(
        telemetry_expected_obj,
        telemetry_output_obj,
        ignore_order=True,
        exclude_paths=exclude_telemetry_paths,
    )

    assert diff_telemetry == {}
    assert isinstance(
        telemetry_output_obj.get("message")
        .get("metadata")
        .get("snowpark_checkpoints_version"),
        str,
    )


def get_expected(file_name: str) -> str:
    current_directory_path = os.path.dirname(__file__)
    expected_file_path = os.path.join(
        current_directory_path,
        TEST_TELEMETRY_VALIDATORS_EXPECTED_DIRECTORY_NAME,
        file_name,
    )

    with open(expected_file_path) as f:
        return f.read().strip()

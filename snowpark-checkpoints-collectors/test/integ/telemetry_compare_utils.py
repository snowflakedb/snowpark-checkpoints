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
from pathlib import Path
from snowflake.snowpark import Session
from snowflake.snowpark_checkpoints_collector.utils.telemetry import TelemetryManager


def get_output_telemetry(telemetry_directory_path: Path) -> str:
    files = os.listdir(telemetry_directory_path)
    files.sort(reverse=True)
    for file in files:
        if file.endswith(".json"):
            output_file_path = os.path.join(telemetry_directory_path, file)
            with open(output_file_path) as f:
                return f.read().strip()
    return "{}"


def validate_telemetry_file_output(
    telemetry_file_name: str, output_path: Path, telemetry_expected_folder: str
) -> None:
    telemetry_expected = get_expected(telemetry_file_name, telemetry_expected_folder)
    telemetry_output = get_output_telemetry(output_path)
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


def get_expected(file_name: str, telemetry_expected_folder: str) -> str:
    current_directory_path = os.path.dirname(__file__)
    expected_file_path = os.path.join(
        current_directory_path,
        telemetry_expected_folder,
        file_name,
    )

    with open(expected_file_path) as f:
        return f.read().strip()


def reset_telemetry_util():
    connection = Session.builder.getOrCreate().connection
    connection._telemetry = TelemetryManager(
        connection._rest, connection.telemetry_enabled
    )
    return connection._telemetry

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import json
from deepdiff import DeepDiff
from pathlib import Path


def get_output_telemetry(telemetry_directory_path: Path) -> str:
    for file in os.listdir(telemetry_directory_path):
        if file.endswith(".json"):
            output_file_path = os.path.join(telemetry_directory_path, file)
            with open(output_file_path) as f:
                return f.read().strip()
    return "{}"


def validate_telemetry_file_output(
    telemetry_file_name: str, output_path: Path, telemetry_expected_folder: str
) -> None:
    telemetry_output = get_output_telemetry(output_path)
    current_directory_path = os.path.dirname(__file__)
    expected_file_path = os.path.join(
        current_directory_path,
        telemetry_expected_folder,
        telemetry_file_name,
    )

    with open(expected_file_path, "w") as f:
        f.write(telemetry_output)

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

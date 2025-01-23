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


def get_expected(file_name: str) -> str:
    current_directory_path = os.path.dirname(__file__)
    expected_file_path = os.path.join(
        current_directory_path,
        TEST_TELEMETRY_VALIDATORS_EXPECTED_DIRECTORY_NAME,
        file_name,
    )

    with open(expected_file_path) as f:
        return f.read().strip()

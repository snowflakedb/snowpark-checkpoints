#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import hashlib
import json

from os import getenv, makedirs, path
from pathlib import Path
from platform import python_version
from sys import platform
from uuid import getnode

from snowflake.connector import (
    SNOWFLAKE_CONNECTOR_VERSION,
    SnowflakeConnection,
    time_util,
)
from snowflake.connector.constants import DIRS as snowflake_dirs
from snowflake.snowpark.session import Session
from snowflake.snowpark_checkpoints.utils.singleton import Singleton


class TelemetryManager(metaclass=Singleton):
    def __init__(self):
        self.folder_path = str(
            snowflake_dirs.user_config_path / "snowpark-checkpoints-telemetry"
        )
        self.sf_path_telemetry = "/telemetry/send"
        self.flush_size = 25
        self.conn = Session.builder.getOrCreate().connection
        self.is_enabled = self._is_telemetry_enabled()
        self.memory_limit = 5 * 1024 * 1024
        self._upload_local_telemetry()
        self.log_batch = []

    def log_error(self, event_name: str, parameters_info: dict = None):
        self._log_telemetry(event_name, "error", parameters_info)

    def log_info(self, event_name: str, parameters_info=None):
        # if randint(1, 100) <= 5:
        self._log_telemetry(event_name, "info", parameters_info)

    def _log_telemetry(self, event_name: str, event_type, parameters_info=None) -> dict:
        if not self.is_enabled:
            return None
        event = _generate_event(event_name, event_type, self.conn, parameters_info)
        self._add_log_to_batch(event)
        return event

    def _add_log_to_batch(self, event: dict) -> None:
        self.log_batch.append(event)
        if len(self.log_batch) >= self.flush_size:
            self._send_batch(self.log_batch)
            self.log_batch = []

    def _send_batch(self, to_sent: list) -> bool:
        if not self.is_enabled:
            return False
        if self.conn.rest is None:
            self._write_telemetry(to_sent)
            return False
        body = {"logs": to_sent}

        ret = self.conn.rest.request(
            self.sf_path_telemetry,
            body=body,
            method="post",
            client=None,
            timeout=5,
        )
        if not ret.get("success"):
            self._write_telemetry(to_sent)
            return False
        return True

    def _write_telemetry(self, batch: list) -> None:
        makedirs(self.folder_path, exist_ok=True)
        for event in batch:
            message = event.get("message")
            if message is not None:
                file_path = path.join(
                    self.folder_path,
                    f'{datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}-telemetry_{message.get("type")}.json',
                )
                json_content = self._validate_folder_space(event)
                with open(file_path, "w") as json_file:
                    json_file.write(json_content)

    def _validate_folder_space(self, event: dict):
        json_content = json.dumps(event, indent=4, sort_keys=True)
        new_json_file_size = len(json_content.encode("utf-8"))
        telemetry_folder = Path(self.folder_path)
        folder_size = _get_folder_size(telemetry_folder)
        if folder_size + new_json_file_size > self.memory_limit:
            _free_up_space(telemetry_folder, self.memory_limit - new_json_file_size)
        return json_content

    def _upload_local_telemetry(self) -> None:
        batch = []
        for file in Path(self.folder_path).glob("*.json"):
            with open(file) as json_file:
                data_dict = json.load(json_file)
                batch.append(data_dict)
        body = {"logs": batch}
        ret = self.conn.rest.request(
            self.sf_path_telemetry,
            body=body,
            method="post",
            client=None,
            timeout=5,
        )
        if ret.get("success"):
            for file in Path(self.folder_path).glob("*.json"):
                file.unlink()

    def _is_telemetry_enabled(self) -> bool:
        if getenv("SNOWPARK_CHECKPOINTS_TELEMETRY_ENABLED") == "false":
            return False
        return self.conn.telemetry_enabled


def _generate_event(
    event_name: str,
    event_type: str,
    conn: SnowflakeConnection,
    parameters_info: dict = None,
) -> dict:
    metadata = _get_metadata()
    message = {
        "type": event_type,
        "event_name": event_name,
        "driver_type": conn.application,
        "driver_version": SNOWFLAKE_CONNECTOR_VERSION,
        "source": "snowpark-checkpoints",
        "metadata": metadata,
        "data": json.dumps(parameters_info or {}, indent=4),
    }
    timestamp = time_util.get_time_millis()
    event_base = {"message": message, "timestamp": str(timestamp)}

    return event_base


def _get_metadata() -> dict:
    return {
        "OS_Version": platform,
        "Python_Version": python_version(),
        "Device_ID": _get_unique_id(),
    }


def _get_folder_size(folder_path: Path) -> int:
    return sum(f.stat().st_size for f in folder_path.glob("*.json") if f.is_file())


def _free_up_space(folder_path: Path, max_size: int) -> None:
    files = sorted(folder_path.glob("*.json"), key=lambda f: f.stat().st_mtime)
    current_size = _get_folder_size(folder_path)
    for file in files:
        if current_size <= max_size:
            break
        else:
            current_size -= file.stat().st_size
            file.unlink()


def _get_unique_id() -> str:
    node_id_str = str(getnode())
    hashed_id = hashlib.sha256(node_id_str.encode()).hexdigest()
    return hashed_id

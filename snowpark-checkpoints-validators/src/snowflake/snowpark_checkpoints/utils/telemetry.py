#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import hashlib
import inspect
import json

from enum import IntEnum
from os import getcwd, getenv, makedirs, path
from pathlib import Path
from platform import python_version
from sys import platform
from typing import Any, Callable, Optional, TypeVar
from uuid import getnode

from pyspark.sql import dataframe as spark_dataframe

from snowflake.connector import (
    SNOWFLAKE_CONNECTOR_VERSION,
    time_util,
)
from snowflake.connector.constants import DIRS as SNOWFLAKE_DIRS
from snowflake.connector.network import SnowflakeRestful
from snowflake.connector.telemetry import TelemetryClient
from snowflake.snowpark import VERSION as SNOWPARK_VERSION
from snowflake.snowpark import dataframe as snowpark_dataframe
from snowflake.snowpark.session import Session


class TelemetryManager(TelemetryClient):
    def __init__(self, rest: SnowflakeRestful):
        """TelemetryManager class to log telemetry events."""
        super().__init__(rest)
        self.sc_folder_path = str(
            SNOWFLAKE_DIRS.user_config_path / "snowpark-checkpoints-telemetry"
        )
        self.sc_sf_path_telemetry = "/telemetry/send"
        self.sc_flush_size = 25
        self.sc_is_enabled = self._sc_is_telemetry_enabled()
        self.sc_is_testing = self._sc_is_telemetry_testing()
        self.sc_memory_limit = 5 * 1024 * 1024
        self._sc_upload_local_telemetry()
        self.sc_log_batch = []
        self.sc_hypothesis_input_events = []

    def sc_log_error(
        self, event_name: str, parameters_info: Optional[dict] = None
    ) -> None:
        """Log an error telemetry event.

        Args:
            event_name (str): The name of the event.
            parameters_info (dict, optional): Additional parameters for the event. Defaults to None.

        """
        self._sc_log_telemetry(event_name, "error", parameters_info)

    def sc_log_info(
        self, event_name: str, parameters_info: Optional[dict] = None
    ) -> None:
        """Log an information telemetry event.

        Args:
            event_name (str): The name of the event.
            parameters_info (dict, optional): Additional parameters for the event. Defaults to None.

        """
        self._sc_log_telemetry(event_name, "info", parameters_info)

    def _sc_log_telemetry(
        self, event_name: str, event_type: str, parameters_info: Optional[dict] = None
    ) -> dict:
        """Log a telemetry event if is enabled.

        Args:
            event_name (str): The name of the event.
            event_type (str): The type of the event (e.g., "error", "info").
            parameters_info (dict, optional): Additional parameters for the event. Defaults to None.

        Returns:
            dict: The logged event.

        """
        if not self.sc_is_enabled:
            return {}
        event = _generate_event(event_name, event_type, parameters_info)
        self._sc_add_log_to_batch(event)
        return event

    def _sc_add_log_to_batch(self, event: dict) -> None:
        """Add a log event to the batch. If the batch is full, send the events to the API.

        Args:
            event (dict): The event to add.

        """
        self.sc_log_batch.append(event)
        if self.sc_is_testing:
            output_folder = str(
                Path(getcwd()) / "snowpark-checkpoints-output" / "telemetry"
            )
            self._sc_write_telemetry(self.sc_log_batch, output_folder=output_folder)
            self.sc_log_batch = []
            return

        if len(self.sc_log_batch) >= self.sc_flush_size:
            self._sc_send_batch(self.sc_log_batch)
            self.sc_log_batch = []

    def _sc_send_batch(self, to_sent: list) -> bool:
        """Send a request to the API to upload the events. If not have connection, write the events to local folder.

        Args:
            to_sent (list): The batch of events to send.

        Returns:
            bool: True if the batch was sent successfully, False otherwise.

        """
        if not self.sc_is_enabled:
            return False
        if self._rest is None:
            self._sc_write_telemetry(to_sent)
            self.sc_log_batch = []
            return False
        body = {"logs": to_sent}
        ret = self._rest.request(
            self.sc_sf_path_telemetry,
            body=body,
            method="post",
            client=None,
            timeout=5,
        )
        if not ret.get("success"):
            self._sc_write_telemetry(to_sent)
            self.sc_log_batch = []
            return False
        return True

    def _sc_write_telemetry(
        self, batch: list, output_folder: Optional[str] = None
    ) -> None:
        """Write telemetry events to local folder. If the folder is full, free up space for the new events.

        Args:
            batch (list): The batch of events to write.
            output_folder (str, optional): folder used for testing.


        """
        try:
            folder = output_folder or self.sc_folder_path
            makedirs(folder, exist_ok=True)
            for event in batch:
                message = event.get("message")
                if message is not None:
                    file_path = path.join(
                        folder,
                        f'{datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S:%f")}-telemetry_{message.get("type")}.json',
                    )
                    json_content = self._sc_validate_folder_space(event)
                    with open(file_path, "w") as json_file:
                        json_file.write(json_content)
        except Exception:
            pass

    def _sc_validate_folder_space(self, event: dict) -> str:
        """Validate and manage folder space for the new telemetry events.

        Args:
            event (dict): The event to validate.

        Returns:
            str: The JSON content of the event.

        """
        json_content = json.dumps(event, indent=4, sort_keys=True)
        new_json_file_size = len(json_content.encode("utf-8"))
        telemetry_folder = Path(self.sc_folder_path)
        folder_size = _get_folder_size(telemetry_folder)
        if folder_size + new_json_file_size > self.sc_memory_limit:
            _free_up_space(telemetry_folder, self.sc_memory_limit - new_json_file_size)
        return json_content

    def _sc_upload_local_telemetry(self) -> None:
        """Send a request to the API to upload the local telemetry events."""
        if not self.is_enabled() or self.sc_is_testing:
            return
        batch = []
        for file in Path(self.sc_folder_path).glob("*.json"):
            with open(file) as json_file:
                data_dict = json.load(json_file)
                batch.append(data_dict)
        body = {"logs": batch}
        ret = self._rest.request(
            self.sc_sf_path_telemetry,
            body=body,
            method="post",
            client=None,
            timeout=5,
        )
        if ret.get("success"):
            for file in Path(self.sc_folder_path).glob("*.json"):
                file.unlink()

    def _sc_is_telemetry_enabled(self) -> bool:
        """Check if telemetry is enabled.

        Returns:
            bool: True if telemetry is enabled, False otherwise.

        """
        if self._sc_is_telemetry_testing():
            return True
        if getenv("SNOWPARK_CHECKPOINTS_TELEMETRY_ENABLED") == "false":
            return False
        return self._rest is not None

    def _sc_is_telemetry_testing(self) -> bool:
        if getenv("SNOWPARK_CHECKPOINTS_TELEMETRY_TESTING") == "true":
            return True
        return False

    def _sc_is_telemetry_manager(self) -> bool:
        """Check if the class is telemetry manager.

        Returns:
            bool: True if the class is telemetry manager, False otherwise.

        """
        return True

    def sc_is_hypothesis_event_logged(self, event_name: str) -> bool:
        """Check if the event is logged.

        Args:
            event_name (str): The name of the event.

        Returns:
            bool: True if the event is logged, False otherwise.

        """
        return event_name in self.sc_hypothesis_input_events


def _generate_event(
    event_name: str,
    event_type: str,
    parameters_info: Optional[dict] = None,
) -> dict:
    """Generate a telemetry event.

    Args:
        event_name (str): The name of the event.
        event_type (str): The type of the event (e.g., "error", "info").
        parameters_info (dict, optional): Additional parameters for the event. Defaults to None.

    Returns:
        dict: The generated event.

    """
    metadata = _get_metadata()
    message = {
        "type": event_type,
        "event_name": event_name,
        "driver_type": "PythonConnector",
        "driver_version": SNOWFLAKE_CONNECTOR_VERSION,
        "source": "snowpark-checkpoints",
        "metadata": metadata,
        "data": json.dumps(parameters_info or {}),
    }
    timestamp = time_util.get_time_millis()
    event_base = {"message": message, "timestamp": str(timestamp)}

    return event_base


def _get_metadata() -> dict:
    """Get metadata for telemetry events.

    Returns:
        dict: The metadata including OS version, Python version, and device ID.

    """
    return {
        "os_version": platform,
        "python_version": python_version(),
        "snowpark_version": ".".join(str(x) for x in SNOWPARK_VERSION if x is not None),
        "device_id": _get_unique_id(),
    }


def _get_folder_size(folder_path: Path) -> int:
    """Get the size of a folder. Only considers JSON files.

    Args:
        folder_path (Path): The path to the folder.

    Returns:
        int: The size of the folder in bytes.

    """
    return sum(f.stat().st_size for f in folder_path.glob("*.json") if f.is_file())


def _free_up_space(folder_path: Path, max_size: int) -> None:
    """Free up space in a folder by deleting the oldest files. Only considers JSON files.

    Args:
        folder_path (Path): The path to the folder.
        max_size (int): The maximum allowed size of the folder in bytes.

    """
    files = sorted(folder_path.glob("*.json"), key=lambda f: f.stat().st_mtime)
    current_size = _get_folder_size(folder_path)
    for file in files:
        if current_size <= max_size:
            break
        current_size -= file.stat().st_size
        file.unlink()


def _get_unique_id() -> str:
    """Get a unique device ID. The ID is generated based on the hashed MAC address.

    Returns:
        str: The hashed device ID.

    """
    node_id_str = str(getnode())
    hashed_id = hashlib.sha256(node_id_str.encode()).hexdigest()
    return hashed_id


def get_telemetry_manager() -> TelemetryManager:
    """Get the telemetry manager.

    Returns:
        TelemetryManager: The telemetry manager.

    """
    connection = Session.builder.getOrCreate().connection
    if not hasattr(connection._telemetry, "_sc_is_telemetry_manager"):
        connection._telemetry = TelemetryManager(connection._rest)
    return connection._telemetry


def get_snowflake_schema_types(df: snowpark_dataframe.DataFrame) -> list[str]:
    """Extract the data types of the schema fields from a Snowflake DataFrame.

    Args:
        df (snowpark_dataframe.DataFrame): The Snowflake DataFrame.

    Returns:
        list[str]: A list of data type names of the schema fields.

    """
    return [str(schema_type.datatype) for schema_type in df.schema.fields]


def get_spark_schema_types(df: spark_dataframe.DataFrame) -> list[str]:
    """Extract the data types of the schema fields from a Spark DataFrame.

    Args:
        df (spark_dataframe.DataFrame): The Spark DataFrame.

    Returns:
        list[str]: A list of data type names of the schema fields.

    """
    return [str(schema_type.dataType) for schema_type in df.schema.fields]


def get_load_json(json_schema: str) -> dict:
    """Load and parse a JSON schema file.

    Args:
        json_schema (str): The path to the JSON schema file.

    Returns:
        dict: The parsed JSON content.

    Raises:
        ValueError: If there is an error reading or parsing the JSON file.

    """
    try:
        with open(json_schema, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, json.JSONDecodeError) as e:
        raise ValueError(f"Error reading JSON schema file: {e}") from None


def extract_parameters(
    func: Callable, args: tuple, kwargs: dict, params_list: Optional[list[str]]
) -> dict:
    """Extract parameters from the function arguments.

    Args:
        func (Callable): The function being decorated.
        args (tuple): The positional arguments passed to the function.
        kwargs (dict): The keyword arguments passed to the function.
        params_list (list[str]): The list of parameters to extract.

    Returns:
        dict: A dictionary of extracted parameters.

    """
    parameters = inspect.signature(func).parameters
    param_data = {}
    if params_list:
        for _, param in enumerate(params_list):
            if len(args) > 0:
                index = list(parameters.keys()).index(param)
                param_data[param] = args[index]
            else:
                if kwargs[param]:
                    param_data[param] = kwargs[param]
    return param_data


def handle_result(
    func_name: str,
    result: Any,
    param_data: dict,
    return_indexes: Optional[list[tuple[str, int]]],
    multiple_return: bool,
    telemetry_m: TelemetryManager,
) -> None:
    """Handle the result of the function and log telemetry data.

    Args:
        func_name (str): The name of the function.
        result: The result of the function.
        param_data (dict): The extracted parameters.
        return_indexes (list[tuple[str, int]]): The list of return values to report.
        multiple_return (bool): Whether the function returns multiple values.
        telemetry_m (TelemetryManager): The telemetry manager.

    """
    if result and return_indexes:
        if multiple_return:
            for name, index in return_indexes:
                param_data[name] = result[index]
        else:
            param_data[return_indexes[0][0]] = result[return_indexes[0][1]]

    telemetry_data = {
        FUNCTION_KEY: func_name,
    }

    if func_name == "_check_dataframe_schema":
        telemetry_data[STATUS_KEY] = param_data[STATUS_KEY]
        telemetry_data[SCHEMA_TYPES_KEY] = list(
            param_data["pandera_schema"].columns.keys()
        )
        telemetry_m.sc_log_info(DATAFRAME_VALIDATOR_SCHEMA, telemetry_data)
    elif func_name in ["check_output_schema", "check_input_schema"]:
        telemetry_data[SCHEMA_TYPES_KEY] = list(
            param_data["pandera_schema"].columns.keys()
        )
        telemetry_m.sc_log_info(DATAFRAME_VALIDATOR_SCHEMA, telemetry_data)
    elif func_name == "_collect_dataframe_checkpoint_mode_schema":
        telemetry_data[MODE_KEY] = CheckpointMode.SCHEMA.value
        schema_types = param_data["column_type_dict"]
        telemetry_data[SCHEMA_TYPES_KEY] = [
            schema_types[schema_type] for schema_type in schema_types
        ]
        telemetry_m.sc_log_info(DATAFRAME_COLLECTION, telemetry_data)
    elif func_name == "_assert_return":
        telemetry_data[STATUS_KEY] = param_data[STATUS_KEY]
        if isinstance(
            param_data["snowpark_results"], snowpark_dataframe.DataFrame
        ) and isinstance(param_data["spark_results"], spark_dataframe.DataFrame):
            telemetry_data[SNOWFLAKE_SCHEMA_TYPES_KEY] = get_snowflake_schema_types(
                param_data["snowpark_results"]
            )
            telemetry_data[SPARK_SCHEMA_TYPES_KEY] = get_spark_schema_types(
                param_data["spark_results"]
            )
            telemetry_m.sc_log_info(DATAFRAME_VALIDATOR_MIRROR, telemetry_data)
        else:
            telemetry_m.sc_log_info(VALUE_VALIDATOR_MIRROR, telemetry_data)
    elif func_name == "dataframe_strategy":
        is_logged = telemetry_m.sc_is_hypothesis_event_logged(param_data["json_schema"])
        if not is_logged:
            json_data = get_load_json(param_data["json_schema"])["custom_data"][
                "columns"
            ]
            telemetry_data[SCHEMA_TYPES_KEY] = [column["type"] for column in json_data]
            telemetry_m.sc_log_info(HYPOTHESIS_INPUT_SCHEMA, telemetry_data)
            telemetry_m.sc_hypothesis_input_events.append(param_data["json_schema"])


def handle_exception(func_name: str, param_data: dict, err: Exception) -> None:
    """Handle exceptions raised by the function and log telemetry data.

    Args:
        func_name (str): The name of the function.
        param_data (dict): The extracted parameters.
        err (Exception): The exception raised by the function.

    """
    telemetry_m = get_telemetry_manager()
    telemetry_data = {
        FUNCTION_KEY: func_name,
        ERROR_KEY: type(err).__name__,
    }
    if func_name == "_collect_dataframe_checkpoint_mode_schema":
        schema_types = param_data["column_type_dict"]
        if schema_types:
            telemetry_data[SCHEMA_TYPES_KEY] = [
                schema_types[schema_type] for schema_type in schema_types
            ]
        telemetry_m.sc_log_error(DATAFRAME_COLLECTION_ERROR, telemetry_data)
    elif func_name in [
        "_check_dataframe_schema",
        "check_output_schema",
        "check_input_schema",
    ]:
        schema_types = param_data["pandera_schema"].columns.keys()
        if schema_types:
            telemetry_data[SCHEMA_TYPES_KEY] = list(schema_types)
        telemetry_m.sc_log_error(DATAFRAME_VALIDATOR_ERROR, telemetry_data)
    elif func_name == "_assert_return":
        if isinstance(param_data["snowpark_results"], snowpark_dataframe.DataFrame):
            telemetry_data[SNOWFLAKE_SCHEMA_TYPES_KEY] = get_snowflake_schema_types(
                param_data["snowpark_results"]
            )
        if isinstance(param_data["spark_results"], spark_dataframe.DataFrame):
            telemetry_data[SPARK_SCHEMA_TYPES_KEY] = get_spark_schema_types(
                param_data["spark_results"]
            )
        telemetry_m.sc_log_error(DATAFRAME_VALIDATOR_ERROR, telemetry_data)
    elif func_name == "dataframe_strategy":
        input_json = param_data["json_schema"]
        if input_json:
            is_logged = telemetry_m.sc_is_hypothesis_event_logged(input_json)
            if not is_logged:
                telemetry_m.sc_log_error(HYPOTHESIS_INPUT_SCHEMA_ERROR, telemetry_data)
                telemetry_m.sc_hypothesis_input_events.append(param_data["json_schema"])


fn = TypeVar("fn", bound=Callable)


def report_telemetry(
    params_list: list[str] = None,
    return_indexes: list[tuple[str, int]] = None,
    multiple_return: bool = False,
) -> Callable[[fn], fn]:
    """Report telemetry events for a function.

    Args:
        params_list (list[str], optional): The list of parameters to report. Defaults to None.
        return_indexes (list[tuple[str, int]], optional): The list of return values to report. Defaults to None.
        multiple_return (bool, optional): Whether the function returns multiple values. Defaults to False.

    Returns:
        Callable[[fn], fn]: The decorator function.

    """

    def report_telemetry_decorator(func):
        func_name = func.__name__

        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
            except Exception as err:
                raise Exception from err

            param_data = {}
            try:
                param_data = extract_parameters(func, args, kwargs, params_list)
                telemetry_m = get_telemetry_manager()
                handle_result(
                    func_name,
                    result,
                    param_data,
                    return_indexes,
                    multiple_return,
                    telemetry_m,
                )
                return result
            except Exception as err:
                handle_exception(func_name, param_data, err)

        return wrapper

    return report_telemetry_decorator


# Constants for telemetry
DATAFRAME_COLLECTION = "DataFrame_Collection"
DATAFRAME_VALIDATOR_MIRROR = "DataFrame_Validator_Mirror"
VALUE_VALIDATOR_MIRROR = "Value_Validator_Mirror"
DATAFRAME_VALIDATOR_SCHEMA = "DataFrame_Validator_Schema"
HYPOTHESIS_INPUT_SCHEMA = "Hypothesis_Input_Schema"
DATAFRAME_COLLECTION_ERROR = "DataFrame_Collection_Error"
DATAFRAME_VALIDATOR_ERROR = "DataFrame_Validator_Error"
HYPOTHESIS_INPUT_SCHEMA_ERROR = "Hypothesis_Input_Schema_Error"

FUNCTION_KEY = "function"
STATUS_KEY = "status"
SCHEMA_TYPES_KEY = "schema_types"
ERROR_KEY = "error"
MODE_KEY = "mode"
SNOWFLAKE_SCHEMA_TYPES_KEY = "snowflake_schema_types"
SPARK_SCHEMA_TYPES_KEY = "spark_schema_types"


class CheckpointMode(IntEnum):
    SCHEMA = 1
    DATAFRAME = 2

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import hashlib
import inspect
import json
import os

from enum import IntEnum
from functools import wraps
from os import getcwd, getenv, makedirs
from pathlib import Path
from platform import python_version
from sys import platform
from typing import Any, Callable, Optional, TypeVar
from uuid import getnode

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


try:
    from pyspark.sql import dataframe as spark_dataframe

    def _is_spark_dataframe(df: Any) -> bool:
        return isinstance(df, spark_dataframe.DataFrame)

    def _get_spark_schema_types(df: spark_dataframe.DataFrame) -> list[str]:
        return [str(schema_type.dataType) for schema_type in df.schema.fields]

except Exception:

    def _is_spark_dataframe(df: Any):
        pass

    def _get_spark_schema_types(df: Any):
        pass


class TelemetryManager(TelemetryClient):
    def __init__(self, rest: SnowflakeRestful):
        """TelemetryManager class to log telemetry events."""
        super().__init__(rest)
        self.sc_folder_path = (
            Path(SNOWFLAKE_DIRS.user_config_path) / "snowpark-checkpoints-telemetry"
        )
        self.sc_sf_path_telemetry = "/telemetry/send"
        self.sc_flush_size = 25
        self.sc_is_enabled = self._sc_is_telemetry_enabled()
        self.sc_is_testing = self._sc_is_telemetry_testing()
        self.sc_memory_limit = 5 * 1024 * 1024
        self._sc_upload_local_telemetry()
        self.sc_log_batch = []
        self.sc_hypothesis_input_events = []

    def set_sc_output_path(self, path: Path) -> None:
        """Set the output path for testing.

        Args:
            path: path to write telemetry.

        """
        os.makedirs(path, exist_ok=True)
        self.sc_folder_path = path

    def sc_log_error(
        self, event_name: str, parameters_info: Optional[dict] = None
    ) -> None:
        """Log an error telemetry event.

        Args:
            event_name (str): The name of the event.
            parameters_info (dict, optional): Additional parameters for the event. Defaults to None.

        """
        if event_name is not None:
            self._sc_log_telemetry(event_name, "error", parameters_info)

    def sc_log_info(
        self, event_name: str, parameters_info: Optional[dict] = None
    ) -> None:
        """Log an information telemetry event.

        Args:
            event_name (str): The name of the event.
            parameters_info (dict, optional): Additional parameters for the event. Defaults to None.

        """
        if event_name is not None:
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
            self._sc_write_telemetry(self.sc_log_batch)
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
        if to_sent == []:
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

    def _sc_write_telemetry(self, batch: list) -> None:
        """Write telemetry events to local folder. If the folder is full, free up space for the new events.

        Args:
            batch (list): The batch of events to write.

        """
        try:
            makedirs(self.sc_folder_path, exist_ok=True)
            for event in batch:
                message = event.get("message")
                if message is not None:
                    file_path = (
                        self.sc_folder_path
                        / f'{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")}'
                        f'_telemetry_{message.get("type")}.json'
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
        telemetry_folder = self.sc_folder_path
        folder_size = _get_folder_size(telemetry_folder)
        if folder_size + new_json_file_size > self.sc_memory_limit:
            _free_up_space(telemetry_folder, self.sc_memory_limit - new_json_file_size)
        return json_content

    def _sc_upload_local_telemetry(self) -> None:
        """Send a request to the API to upload the local telemetry events."""
        if not self.is_enabled() or self.sc_is_testing:
            return
        batch = []
        for file in self.sc_folder_path.glob("*.json"):
            with open(file) as json_file:
                data_dict = json.load(json_file)
                batch.append(data_dict)
        if batch == []:
            return
        body = {"logs": batch}
        ret = self._rest.request(
            self.sc_sf_path_telemetry,
            body=body,
            method="post",
            client=None,
            timeout=5,
        )
        if ret.get("success"):
            for file in self.sc_folder_path.glob("*.json"):
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
        is_testing = getenv("SNOWPARK_CHECKPOINTS_TELEMETRY_TESTING") == "true"
        if is_testing:
            local_telemetry_path = (
                Path(getcwd()) / "snowpark-checkpoints-output" / "telemetry"
            )
            self.set_sc_output_path(local_telemetry_path)
        return is_testing

    def _sc_is_telemetry_manager(self) -> bool:
        """Check if the class is telemetry manager.

        Returns:
            bool: True if the class is telemetry manager, False otherwise.

        """
        return True

    def sc_is_hypothesis_event_logged(self, event_name: tuple[str, int]) -> bool:
        """Check if a Hypothesis event is logged.

        Args:
            event_name (tuple[str, int]): A tuple containing the name of the event and an integer identifier
            (0 for info, 1 for error).

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


def _is_snowpark_dataframe(df: Any) -> bool:
    """Check if the given dataframe is a Snowpark dataframe.

    Args:
        df: The dataframe to check.

    Returns:
        bool: True if the dataframe is a Snowpark dataframe, False otherwise.

    """
    return isinstance(df, snowpark_dataframe.DataFrame)


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


def check_dataframe_schema_event(
    telemetry_data: dict, param_data: dict
) -> tuple[str, dict]:
    """Handle telemetry event for checking dataframe schema.

    Args:
        telemetry_data (dict): The telemetry data dictionary.
        param_data (dict): The parameter data dictionary.

    Returns:
        tuple: A tuple containing the event name and telemetry data.

    """
    try:
        telemetry_data[STATUS_KEY] = param_data.get(STATUS_KEY)
        pandera_schema = param_data.get(PANDER_SCHEMA_PARAM)
        schema_types = []
        for schema_type in pandera_schema.columns.values():
            if schema_type.dtype is not None:
                schema_types.append(str(schema_type.dtype))
        if schema_types:
            telemetry_data[SCHEMA_TYPES_KEY] = schema_types
        return DATAFRAME_VALIDATOR_SCHEMA, telemetry_data
    except Exception:
        if param_data.get(STATUS_KEY):
            telemetry_data[STATUS_KEY] = param_data.get(STATUS_KEY)
        pandera_schema = param_data.get(PANDER_SCHEMA_PARAM)
        if pandera_schema:
            schema_types = []
            for schema_type in pandera_schema.columns.values():
                if schema_type.dtype is not None:
                    schema_types.append(str(schema_type.dtype))
            if schema_types:
                telemetry_data[SCHEMA_TYPES_KEY] = schema_types
        return DATAFRAME_VALIDATOR_ERROR, telemetry_data


def check_output_or_input_schema_event(
    telemetry_data: dict, param_data: dict
) -> tuple[str, dict]:
    """Handle telemetry event for checking output or input schema.

    Args:
        telemetry_data (dict): The telemetry data dictionary.
        param_data (dict): The parameter data dictionary.

    Returns:
        tuple: A tuple containing the event name and telemetry data.

    """
    try:
        pandera_schema = param_data.get(PANDER_SCHEMA_PARAM)
        schema_types = []
        for schema_type in pandera_schema.columns.values():
            if schema_type.dtype is not None:
                schema_types.append(str(schema_type.dtype))
        if schema_types:
            telemetry_data[SCHEMA_TYPES_KEY] = schema_types
        return DATAFRAME_VALIDATOR_SCHEMA, telemetry_data
    except Exception:
        return DATAFRAME_VALIDATOR_ERROR, telemetry_data


def collect_dataframe_checkpoint_mode_schema(
    telemetry_data: dict, param_data: dict
) -> tuple[str, dict]:
    """Handle telemetry event for collecting dataframe checkpoint mode schema.

    Args:
        telemetry_data (dict): The telemetry data dictionary.
        param_data (dict): The parameter data dictionary.

    Returns:
        tuple: A tuple containing the event name and telemetry data.

    """
    try:
        telemetry_data[MODE_KEY] = CheckpointMode.SCHEMA.value
        schema_types = param_data.get("column_type_dict")
        telemetry_data[SCHEMA_TYPES_KEY] = [
            schema_types[schema_type].dataType.typeName()
            for schema_type in schema_types
        ]
        return DATAFRAME_COLLECTION, telemetry_data
    except Exception:
        telemetry_data[MODE_KEY] = CheckpointMode.SCHEMA.value
        return DATAFRAME_COLLECTION_ERROR, telemetry_data


def assert_return_event(telemetry_data: dict, param_data: dict) -> tuple[str, dict]:
    """Handle telemetry event for asserting return values.

    Args:
        telemetry_data (dict): The telemetry data dictionary.
        param_data (dict): The parameter data dictionary.

    Returns:
        tuple: A tuple containing the event name and telemetry data.

    """
    try:
        telemetry_data[STATUS_KEY] = param_data.get(STATUS_KEY)
        if _is_snowpark_dataframe(
            param_data.get(SNOWPARK_RESULTS)
            and _is_spark_dataframe(param_data.get(SPARK_RESULTS))
        ):
            telemetry_data[SNOWFLAKE_SCHEMA_TYPES_KEY] = get_snowflake_schema_types(
                param_data.get(SNOWPARK_RESULTS)
            )
            telemetry_data[SPARK_SCHEMA_TYPES_KEY] = _get_spark_schema_types(
                param_data.get(SPARK_RESULTS)
            )
            return DATAFRAME_VALIDATOR_MIRROR, telemetry_data
        else:
            return VALUE_VALIDATOR_MIRROR, telemetry_data
    except Exception:
        if param_data.get(STATUS_KEY) is not None:
            telemetry_data[STATUS_KEY] = param_data.get(STATUS_KEY)
        if _is_snowpark_dataframe(param_data.get(SNOWPARK_RESULTS)):
            telemetry_data[SNOWFLAKE_SCHEMA_TYPES_KEY] = get_snowflake_schema_types(
                param_data.get(SNOWPARK_RESULTS)
            )
        if _is_spark_dataframe(param_data.get(SPARK_RESULTS)):
            telemetry_data[SPARK_SCHEMA_TYPES_KEY] = _get_spark_schema_types(
                param_data.get(SPARK_RESULTS)
            )
        return DATAFRAME_VALIDATOR_ERROR, telemetry_data


def dataframe_strategy_event(
    telemetry_data: dict, param_data: dict, telemetry_m: TelemetryManager
) -> tuple[Optional[str], Optional[dict]]:
    """Handle telemetry event for dataframe strategy.

    Args:
        telemetry_data (dict): The telemetry data dictionary.
        param_data (dict): The parameter data dictionary.
        telemetry_m (TelemetryManager): The telemetry manager.

    Returns:
        tuple: A tuple containing the event name and telemetry data.

    """
    try:
        test_function_name = inspect.stack()[2].function
        is_logged = telemetry_m.sc_is_hypothesis_event_logged((test_function_name, 0))
        if not is_logged:
            schema_param = param_data.get(DATAFRAME_STRATEGY_SCHEMA_PARAM_NAME)
            if isinstance(schema_param, str):
                json_data = get_load_json(schema_param)["custom_data"]["columns"]
                telemetry_data[SCHEMA_TYPES_KEY] = [
                    column["type"] for column in json_data
                ]
            else:
                schema_types = []
                for schema_type in schema_param.columns.values():
                    if schema_type.dtype is not None:
                        schema_types.append(str(schema_type.dtype))
                if schema_types:
                    telemetry_data[SCHEMA_TYPES_KEY] = schema_types
            telemetry_m.sc_hypothesis_input_events.append((test_function_name, 0))
            if None in telemetry_data[SCHEMA_TYPES_KEY]:
                telemetry_m.sc_log_error(HYPOTHESIS_INPUT_SCHEMA_ERROR, telemetry_data)
            else:
                telemetry_m.sc_log_info(HYPOTHESIS_INPUT_SCHEMA, telemetry_data)
            telemetry_m._sc_send_batch(telemetry_m.sc_log_batch)
        return None, None
    except Exception:
        test_function_name = inspect.stack()[2].function
        is_logged = telemetry_m.sc_is_hypothesis_event_logged((test_function_name, 1))
        if not is_logged:
            telemetry_m.sc_hypothesis_input_events.append((test_function_name, 0))
            telemetry_m.sc_log_error(HYPOTHESIS_INPUT_SCHEMA_ERROR, telemetry_data)
            telemetry_m._sc_send_batch(telemetry_m.sc_log_batch)
        return None, None


def handle_result(
    func_name: str,
    result: Any,
    param_data: dict,
    multiple_return: bool,
    telemetry_m: TelemetryManager,
    return_indexes: Optional[list[tuple[str, int]]] = None,
) -> tuple[Optional[str], Optional[dict]]:
    """Handle the result of the function and collect telemetry data.

    Args:
        func_name (str): The name of the function.
        result: The result of the function.
        param_data (dict): The extracted parameters.
        multiple_return (bool): Whether the function returns multiple values.
        telemetry_m (TelemetryManager): The telemetry manager.
        return_indexes (list[tuple[str, int]]): The list of return values to report. Defaults to None.

    Returns:
        tuple: A tuple containing the event name (str) and telemetry data (dict).

    """
    if result is not None and return_indexes is not None:
        if multiple_return:
            for name, index in return_indexes:
                param_data[name] = result[index]
        else:
            param_data[return_indexes[0][0]] = result[return_indexes[0][1]]

    telemetry_data = {
        FUNCTION_KEY: func_name,
    }

    telemetry_event = None
    data = None
    if func_name == "_check_dataframe_schema":
        telemetry_event, data = check_dataframe_schema_event(telemetry_data, param_data)
    elif func_name in ["check_output_schema", "check_input_schema"]:
        telemetry_event, data = check_output_or_input_schema_event(
            telemetry_data, param_data
        )
    elif func_name == "_collect_dataframe_checkpoint_mode_schema":
        telemetry_event, data = collect_dataframe_checkpoint_mode_schema(
            telemetry_data, param_data
        )
    elif func_name == "_assert_return":
        telemetry_event, data = assert_return_event(telemetry_data, param_data)
    elif func_name == "dataframe_strategy":
        telemetry_event, data = dataframe_strategy_event(
            telemetry_data, param_data, telemetry_m
        )
    return telemetry_event, data


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

        @wraps(func)
        def wrapper(*args, **kwargs):
            func_exception = None
            result = None
            try:
                result = func(*args, **kwargs)
            except Exception as err:
                func_exception = err

            telemetry_event = None
            data = None
            telemetry_m = None
            try:
                param_data = extract_parameters(func, args, kwargs, params_list)
                telemetry_m = get_telemetry_manager()
                telemetry_event, data = handle_result(
                    func_name,
                    result,
                    param_data,
                    multiple_return,
                    telemetry_m,
                    return_indexes,
                )
            except Exception:
                # Report error in telemetry
                telemetry_event = TELEMETRY_REPORT_ERROR
                data = {
                    FUNCTION_KEY: func_name,
                }
            finally:
                if func_exception is not None:
                    if telemetry_m is not None:
                        telemetry_m.sc_log_error(telemetry_event, data)
                    raise func_exception
                if telemetry_m is not None:
                    telemetry_m.sc_log_info(telemetry_event, data)

            return result

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
TELEMETRY_REPORT_ERROR = "Telemetry_Report_Error"

FUNCTION_KEY = "function"
STATUS_KEY = "status"
SCHEMA_TYPES_KEY = "schema_types"
ERROR_KEY = "error"
MODE_KEY = "mode"
SNOWFLAKE_SCHEMA_TYPES_KEY = "snowflake_schema_types"
SPARK_SCHEMA_TYPES_KEY = "spark_schema_types"

DATAFRAME_STRATEGY_SCHEMA_PARAM_NAME = "schema"
PANDER_SCHEMA_PARAM = "pandera_schema"
SNOWPARK_RESULTS = "snowpark_results"
SPARK_RESULTS = "spark_results"


class CheckpointMode(IntEnum):
    SCHEMA = 1
    DATAFRAME = 2

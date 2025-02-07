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
import tempfile
from unittest.mock import MagicMock, patch, mock_open
import unittest
from deepdiff import DeepDiff

from snowflake.snowpark_checkpoints.utils.telemetry import report_telemetry


class TelemetryManagerTest(unittest.TestCase):
    def test_get_telemetry_manager_no_setted(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import (
            TelemetryManager,
            get_telemetry_manager,
        )
        from snowflake.connector.telemetry import TelemetryClient

        _, mock_DIRS = mock_before_telemetry_import()
        session_mock = MagicMock()
        connection_mock = MagicMock(_telemetry=MagicMock(spec=TelemetryClient))
        session_mock.builder.getOrCreate.return_value = MagicMock(
            connection=connection_mock
        )
        type_mock = MagicMock()
        type_mock.__name__ = "TelemetryClient"

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", session_mock
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ):
            # Act
            result = get_telemetry_manager()
            result2 = get_telemetry_manager()

            # Assert
            assert isinstance(result, TelemetryManager)
            assert result is result2

    def test_get_telemetry_manager_no_connection(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import (
            TelemetryManager,
            get_telemetry_manager,
        )
        from pathlib import Path

        session_mock = MagicMock()
        session_mock.builder.getOrCreate.side_effect = Exception("No connection")
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        event_return = {"testing": "boo"}

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", session_mock
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_write_telemetry",
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._generate_event",
            return_value=event_return,
        ), patch.object(
            Path, "glob"
        ):
            # Act
            result = get_telemetry_manager()
            result.sc_log_info("event_name", event_return)

            # Assert
            assert isinstance(result, TelemetryManager)
            TelemetryManager._sc_write_telemetry.assert_called_once_with([event_return])
            assert result.sc_log_batch == []
            Path.glob.assert_not_called()

    def test_get_telemetry_manager_no_connection_with_sc_is_telemetry_testing(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import (
            TelemetryManager,
            get_telemetry_manager,
        )
        from pathlib import Path

        session_mock = MagicMock()
        session_mock.builder.getOrCreate.side_effect = Exception("No connection")
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        event_return = {"testing": "boo"}

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", session_mock
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_write_telemetry",
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._generate_event",
            return_value=event_return,
        ), patch.object(
            Path, "glob"
        ):
            # Act
            result = get_telemetry_manager()
            result.sc_log_info("event_name", event_return)

            # Assert
            assert isinstance(result, TelemetryManager)
            TelemetryManager._sc_write_telemetry.assert_called_once_with([event_return])
            assert result.sc_log_batch == []
            Path.glob.assert_not_called()

    def test_get_metadata(self):
        # Arrange
        expected_os = "os"
        expected_python_version = "0.0.0"
        expected_unique_id = "1234"
        expected_snowpark_version = "0.0.0"

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.platform", expected_os
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.python_version",
            return_value=expected_python_version,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWPARK_VERSION",
            (0, 0, 0),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._get_unique_id",
            return_value=expected_unique_id,
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import _get_metadata

            # Act
            result = _get_metadata()

            # Assert
            assert result.get("os_version") == expected_os
            assert result.get("python_version") == expected_python_version
            assert result.get("snowpark_version") == expected_snowpark_version
            assert result.get("device_id") == expected_unique_id

    def test_get_version(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import _get_version

        # Act
        result = _get_version()

        # Assert
        assert isinstance(result, str)

    def test_get_unique_id(self):
        # Arrange
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.getnode", return_value=1234
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import _get_unique_id

            expected_id = (
                "03ac674216f3e15c761ee1a5e255f067953623c8b388b4459e13f978d7c846f4"
            )

            # Act
            result = _get_unique_id()

            # Assert
            assert result == expected_id

    def test_free_up_space(self):
        # Arrange
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._get_folder_size",
            return_value=100,
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import _free_up_space

            folder_path, json_path = mock_folder_path(MagicMock(st_size=50, st_mtime=1))
            max_size = 50

            # Act
            _free_up_space(folder_path, max_size)

            # Assert
            folder_path.glob.assert_called_once_with("*.json")
            json_path.unlink.assert_called_once()

    def test_get_folder_size(self):
        # Arrange
        folder_path, json_path = mock_folder_path(MagicMock(st_size=50))
        from snowflake.snowpark_checkpoints.utils.telemetry import _get_folder_size

        expected_value = 100

        # Act
        result = _get_folder_size(folder_path)

        # Assert
        assert result == expected_value
        assert result == expected_value

    def test_generate_event(self):
        # Arrange
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_CONNECTOR_VERSION",
            "0.0.0",
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._get_metadata",
            return_value={},
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.time_util.get_time_millis",
            return_value=0,
        ):

            mock_connection = MagicMock()
            mock_connection.application = "foo"
            from snowflake.snowpark_checkpoints.utils.telemetry import _generate_event

            snowpark_checkpoints_version = "0.0.0"

            # Act
            result = _generate_event(
                "event_name",
                "event_type",
                {"testing": "boo"},
                snowpark_checkpoints_version,
            )

            # Assert
            assert result.get("message").get("event_type") == "event_type"
            assert result.get("message").get("event_name") == "event_name"
            assert result.get("message").get("driver_type") == "PythonConnector"
            assert result.get("message").get("type") == "snowpark-checkpoints"
            assert result.get("message").get("metadata") == {
                "snowpark_checkpoints_version": snowpark_checkpoints_version
            }
            assert result.get("message").get("data") == '{"testing": "boo"}'
            assert result.get("timestamp") == "0"
            assert result.get("message").get("driver_version") == "0.0.0"

    def test_telemetry_manager_upload_local_telemetry_success(self):
        # Arrange
        from pathlib import Path

        rest_mock, mock_DIRS = mock_before_telemetry_import()

        _, json_path = mock_folder_path(MagicMock(st_size=50))

        with (
            patch(
                "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS",
                mock_DIRS,
            ),
            patch(
                "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
                return_value=False,
            ),
            patch("builtins.open", mock_open(read_data='{"foo": "bar"}')),
            patch.object(Path, "glob", return_value=[json_path]),
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the upload_local_telemetry method in the __init__ method
            TelemetryManager(rest_mock)

            # Assert
            rest_mock.request.assert_called_with(
                "/telemetry/send",
                body={"logs": [{"foo": "bar"}]},
                method="post",
                client=None,
                timeout=5,
            )
            json_path.unlink.assert_called_once()

    def test_telemetry_manager_upload_local_telemetry_failed(self):
        # Arrange
        from pathlib import Path

        rest_mock, mock_DIRS = mock_before_telemetry_import(request_return=False)
        _, json_path = mock_folder_path(MagicMock(st_size=50))
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "builtins.open", mock_open(read_data='{"foo": "bar"}')
        ), patch.object(
            Path, "glob", return_value=[json_path]
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the upload_local_telemetry method in the __init__ method
            TelemetryManager(rest_mock)

            # Assert
            rest_mock.request.assert_called_with(
                "/telemetry/send",
                body={"logs": [{"foo": "bar"}]},
                method="post",
                client=None,
                timeout=5,
            )
            json_path.unlink.assert_not_called()

    def test_telemetry_manager_is_telemetry_enabled(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.os.getenv",
            return_value=None,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the is_telemetry_enabled method in the __init__ method
            telemetry = TelemetryManager(rest_mock)

            # Assert
            assert telemetry.sc_is_enabled == True
            TelemetryManager._sc_upload_local_telemetry.assert_called_once()

    def test_telemetry_manager_is_telemetry_disabled(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.os.getenv",
            return_value=None,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the is_telemetry_enabled method in the __init__ method
            telemetry = TelemetryManager(None, is_telemetry_enabled=False)

            # Assert
            assert telemetry.sc_is_enabled == False
            TelemetryManager._sc_upload_local_telemetry.assert_called_once()

    def test_telemetry_manager_is_telemetry_disabled_env(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        rest_mock.telemetry_enabled = True
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.os.getenv",
            return_value="false",
        ) as get_env_patch, patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.extract_parameters",
        ) as extract_parameters_patch:

            # Act
            @report_telemetry()
            def foo():
                return True

            foo()

            # Assert
            get_env_patch.assert_called_once_with(
                "SNOWPARK_CHECKPOINTS_TELEMETRY_ENABLED"
            )
            extract_parameters_patch.assert_not_called()

    def test_telemetry_manager_write_telemetry(self):
        # Arrange
        mock_date = "00-00-0000_00-00-00"
        event = {"message": {"type": "test"}}
        batch = [event]
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_validate_folder_space",
            return_value=f"{batch[0]}",
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._get_version"
        ), patch(
            "builtins.open", mock_open()
        ), patch(
            "datetime.datetime",
            MagicMock(
                now=MagicMock(
                    return_value=MagicMock(strftime=MagicMock(return_value=mock_date))
                )
            ),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager
            from os import path

            telemetry = TelemetryManager(rest_mock)

            # Act
            telemetry._sc_write_telemetry(batch)

            # Assert
            TelemetryManager._sc_validate_folder_space.assert_called_once_with(batch[0])
            open.assert_called_once_with(
                telemetry.sc_folder_path / f"{mock_date}_telemetry_test.json", "w"
            )
            open().write.assert_called_once_with(f"{event}")

    def test_telemetry_manager_send_batch_no_telemetry_disabled(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock, is_telemetry_enabled=False)

            # Act
            result = telemetry.sc_send_batch([{"test": "test"}])

            # Assert
            assert result == False

    def test_telemetry_manager_send_batch_no_telemetry_empty_list(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock, is_telemetry_enabled=False)

            # Act
            result = telemetry.sc_send_batch([])

            # Assert
            assert result == False

    def test_telemetry_manager_send_batch_no_telemetry_rest_none(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(None)

            # Act
            result = telemetry.sc_send_batch([{"test": "test"}])

            # Assert
            assert result == False

    def test_telemetry_manager_send_batch_no_connection(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        rest_mock = None

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_write_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock)

            # Act
            result = telemetry.sc_send_batch([])

            # Assert
            TelemetryManager._sc_write_telemetry.assert_called_once_with([])
            assert result == False

    def test_telemetry_manager_send_batch_request_true(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock)
            event = {"message": {"type": "test"}}
            batch = [event]

            # Act
            result = telemetry.sc_send_batch(batch)

            # Assert
            rest_mock.request.assert_called_with(
                "/telemetry/send",
                body={"logs": batch},
                method="post",
                client=None,
                timeout=5,
            )
            assert result == True

    def test_telemetry_manager_send_batch_request_false(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import(request_return=False)
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_write_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock)
            event = {"message": {"type": "test"}}
            batch = [event]

            # Act
            result = telemetry.sc_send_batch(batch)

            # Assert
            rest_mock.request.assert_called_with(
                "/telemetry/send",
                body={"logs": batch},
                method="post",
                client=None,
                timeout=5,
            )
            TelemetryManager._sc_write_telemetry.assert_called_once_with(batch)
            assert result == False

    def test_telemetry_manager_sc_add_log_to_batch(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import(request_return=False)
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager.sc_send_batch",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock)
            telemetry.sc_log_batch = []
            telemetry.sc_flush_size = 2
            event = {"foo": "boo"}

            # Act
            telemetry._sc_add_log_to_batch(event)

            # Assert
            telemetry.sc_send_batch.assert_not_called()
            assert len(telemetry.sc_log_batch) == 1
            assert telemetry.sc_log_batch[0] == event

    def test_telemetry_manager_sc_add_log_to_batch_and_clean(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import(request_return=False)
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager.sc_send_batch",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock)
            telemetry.sc_log_batch = []
            telemetry.sc_flush_size = 1
            event = {"foo": "boo"}

            # Act
            telemetry._sc_add_log_to_batch(event)

            # Assert
            telemetry.sc_send_batch.assert_called_once_with([event])
            assert len(telemetry.sc_log_batch) == 0
            assert telemetry.sc_log_batch == []

    def test_telemetry_manager_log_telemetry(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_add_log_to_batch",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._generate_event",
            return_value={"foo": "boo"},
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._get_version",
            return_value="0.0.0",
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager
            from snowflake.snowpark_checkpoints.utils.telemetry import _generate_event

            telemetry = TelemetryManager(rest_mock)

            # Act
            result = telemetry._sc_log_telemetry(
                "event_name", "event_type", {"testing": "boo"}
            )

            # Assert
            TelemetryManager._sc_add_log_to_batch.assert_called_once_with(
                {"foo": "boo"}
            )
            _generate_event.assert_called_once_with(
                "event_name", "event_type", {"testing": "boo"}, "0.0.0"
            )
            assert result == {"foo": "boo"}

    def test_telemetry_manager_log_telemetry_disable(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_add_log_to_batch",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._generate_event"
        ) as patch_generate_event:
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock, is_telemetry_enabled=False)

            # Act
            result = telemetry._sc_log_telemetry(
                "event_name", "event_type", {"testing": "boo"}
            )

            # Assert
            assert result == {}
            telemetry._sc_add_log_to_batch.assert_not_called()
            patch_generate_event.assert_not_called()

    def test_telemetry_manager_log_info(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_log_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock)

            # Act
            telemetry.sc_log_info("event_name", {"testing": "boo"})

            # Assert
            TelemetryManager._sc_log_telemetry.assert_called_once_with(
                "event_name", "info", {"testing": "boo"}
            )

    def test_telemetry_manager_log_error(self):
        # Arrange
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_log_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager(rest_mock)

            # Act
            telemetry.sc_log_error("event_name", {"testing": "boo"})

            # Assert
            TelemetryManager._sc_log_telemetry.assert_called_once_with(
                "event_name", "error", {"testing": "boo"}
            )

    def test_telemetry_manager_sc_is_telemetry_manager(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

        rest_mock, mock_DIRS = mock_before_telemetry_import()

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ):
            telemetry = TelemetryManager(rest_mock)

            # Act
            result = telemetry._sc_is_telemetry_manager()

            # Assert
            assert result

    def test_telemetry_manager_sc_close_at_exit(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

        rest_mock, mock_DIRS = mock_before_telemetry_import()

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager.sc_send_batch"
        ):
            telemetry = TelemetryManager(rest_mock)
            batch = [{"foo": "boo"}]

            # Act
            telemetry.sc_log_batch = batch
            telemetry._sc_close_at_exit()

            # Assert
            TelemetryManager.sc_send_batch.assert_called_once_with(batch)

    def test_telemetry_manager_sc_close_at_exit_is_testing_enabled(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

        rest_mock, mock_DIRS = mock_before_telemetry_import()

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager.sc_send_batch"
        ):
            telemetry = TelemetryManager(rest_mock)
            batch = [{"foo": "boo"}]

            # Act
            telemetry.sc_log_batch = batch
            telemetry._sc_close_at_exit()

            # Assert
            TelemetryManager.sc_send_batch.assert_not_called()

    def test_get_snowflake_schema_types(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import (
            get_snowflake_schema_types,
        )
        from snowflake.snowpark import dataframe as snowpark_dataframe
        from snowflake.snowpark.types import LongType, StructField

        snowpark_df = MagicMock(spec=snowpark_dataframe.DataFrame)
        snowpark_df.schema.fields = [StructField("foo", LongType(), True)]

        # Act

        result = get_snowflake_schema_types(snowpark_df)

        # Assert
        assert result == ["LongType()"]

    def test_get_spark_schema_types(self):
        # Arrange
        from snowflake.snowpark_checkpoints.utils.telemetry import (
            _get_spark_schema_types,
        )
        from pyspark.sql import dataframe as spark_dataframe
        from pyspark.sql.types import LongType, StructField

        spark_df = MagicMock(spec=spark_dataframe.DataFrame)
        spark_df.schema.fields = [StructField("foo", LongType(), True)]

        # Act

        result = _get_spark_schema_types(spark_df)

        # Assert
        assert result == ["LongType()"]

    def test_sc_write_telemetry_with_temp_folder(self):
        # Arrange
        from pathlib import Path
        from os import getcwd

        event = {"message": {"type": "test"}}
        batch = [event]
        rest_mock, mock_DIRS = mock_before_telemetry_import()
        mock_date = "00-00-0000-00-00-00"

        with tempfile.TemporaryDirectory(dir=getcwd()) as temp_dir:
            temp_path = Path(temp_dir)
            with patch(
                "snowflake.snowpark_checkpoints.utils.telemetry.SNOWFLAKE_DIRS",
                mock_DIRS,
            ), patch(
                "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_validate_folder_space",
                return_value=f"{json.dumps(event)}",
            ), patch(
                "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_is_telemetry_testing",
                return_value=False,
            ), patch(
                "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._sc_upload_local_telemetry",
                return_value=MagicMock(),
            ), patch(
                "datetime.datetime",
                MagicMock(
                    now=MagicMock(
                        return_value=MagicMock(
                            strftime=MagicMock(return_value=mock_date)
                        )
                    )
                ),
            ):
                from snowflake.snowpark_checkpoints.utils.telemetry import (
                    TelemetryManager,
                )

                telemetry = TelemetryManager(rest_mock)

                # Act
                telemetry.set_sc_output_path(temp_path)
                telemetry._sc_write_telemetry(batch)

                # Assert
                file_path = temp_path / f"{mock_date}_telemetry_test.json"
                self.assertTrue(file_path.exists(), "File was not created")
                with open(file_path) as file:
                    telemetry_output = json.load(file)
                TelemetryManager._sc_validate_folder_space.assert_called_once_with(
                    batch[0]
                )

                diff_telemetry = DeepDiff(
                    event,
                    telemetry_output,
                    ignore_order=True,
                )
                assert diff_telemetry == {}


def mock_folder_path(stat):
    folder_path = MagicMock()
    json_path = MagicMock()
    json_path.stat.return_value = stat
    json_path.is_file.return_value = True
    folder_path.glob.return_value = [json_path, json_path]

    return folder_path, json_path


def mock_before_telemetry_import(request_return=True):
    from pathlib import Path

    rest_mock = MagicMock()
    rest_mock.request.return_value = {"success": request_return}
    mock_DIRS = MagicMock()
    mock_DIRS.user_config_path = Path("folder/")
    return rest_mock, mock_DIRS

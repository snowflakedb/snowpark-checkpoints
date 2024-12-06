#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import MagicMock, patch, mock_open
from snowflake.snowpark_checkpoints.utils.singleton import Singleton
import unittest
import pytest


class TelemetryManagerTest(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def reset_singleton(self):
        from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

        Singleton.reset_instance(TelemetryManager)

    def test_get_metadata(self):
        # Arrange
        expected_os = "os"
        expected_python_version = "0.0.0"
        expected_unique_id = "1234"

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.platform", expected_os
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.python_version",
            return_value=expected_python_version,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._get_unique_id",
            return_value=expected_unique_id,
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import _get_metadata

            # Act
            result = _get_metadata()

            # Assert
            assert result.get("OS_Version") == expected_os
            assert result.get("Python_Version") == expected_python_version
            assert result.get("Device_ID") == expected_unique_id

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

            # Act
            result = _generate_event(
                "event_name", "event_type", mock_connection, {"testing": "boo"}
            )

            # Assert
            assert result.get("message").get("type") == "event_type"
            assert result.get("message").get("event_name") == "event_name"
            assert result.get("message").get("driver_type") == "foo"
            assert result.get("message").get("source") == "snowpark-checkpoints"
            assert result.get("message").get("metadata") == {}
            assert result.get("message").get("data") == '{\n    "testing": "boo"\n}'
            assert result.get("timestamp") == "0"
            assert result.get("message").get("driver_version") == "0.0.0"

    def test_telemetry_manager_upload_local_telemetry_success(self):
        # Arrange
        from pathlib import Path

        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()

        _, json_path = mock_folder_path(MagicMock(st_size=50))

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=False,
        ), patch(
            "builtins.open", mock_open(read_data='{"foo": "bar"}')
        ), patch.object(
            Path, "glob", return_value=[json_path]
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the upload_local_telemetry method in the __init__ method
            TelemetryManager()

            # Assert
            rest_mock.rest.request.assert_called_with(
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

        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import(
            request_return=False
        )
        _, json_path = mock_folder_path(MagicMock(st_size=50))
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "builtins.open", mock_open(read_data='{"foo": "bar"}')
        ), patch.object(
            Path, "glob", return_value=[json_path]
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the upload_local_telemetry method in the __init__ method
            TelemetryManager()

            # Assert
            rest_mock.rest.request.assert_called_with(
                "/telemetry/send",
                body={"logs": [{"foo": "bar"}]},
                method="post",
                client=None,
                timeout=5,
            )
            json_path.unlink.assert_not_called()

    def test_telemetry_manager_is_telemetry_enabled(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        rest_mock.telemetry_enabled = True
        mock_session.builder.getOrCreate().connection = rest_mock
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.getenv",
            return_value=None,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the is_telemetry_enabled method in the __init__ method
            telemetry = TelemetryManager()

            # Assert
            assert telemetry.is_enabled == True
            TelemetryManager._upload_local_telemetry.assert_called_once()

    def test_telemetry_manager_is_telemetry_disable(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        rest_mock.telemetry_enabled = False
        mock_session.builder.getOrCreate().connection = rest_mock
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.getenv",
            return_value=None,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the is_telemetry_enabled method in the __init__ method
            telemetry = TelemetryManager()

            # Assert
            assert telemetry.is_enabled == False
            TelemetryManager._upload_local_telemetry.assert_called_once()

    def test_telemetry_manager_is_telemetry_disable_env(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        rest_mock.telemetry_enabled = True
        mock_session.builder.getOrCreate().connection = rest_mock
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.getenv",
            return_value="false",
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ):

            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            # Act
            # Calls the is_telemetry_enabled method in the __init__ method
            telemetry = TelemetryManager()

            # Assert
            assert telemetry.is_enabled == False
            TelemetryManager._upload_local_telemetry.assert_called_once()

    def test_telemetry_manager_write_telemetry(self):
        # Arrange
        mock_date = "00-00-0000-00:00:00"
        event = {"message": {"type": "test"}}
        batch = [event]
        mock_session, _, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._validate_folder_space",
            return_value=f"{batch[0]}",
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry.makedirs"
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

            telemetry = TelemetryManager()

            # Act
            telemetry._write_telemetry(batch)

            # Assert
            TelemetryManager._validate_folder_space.assert_called_once_with(batch[0])
            open.assert_called_once_with(
                f"{telemetry.folder_path}/{mock_date}-telemetry_test.json", "w"
            )
            open().write.assert_called_once_with(f"{event}")

    def test_telemetry_manager_send_batch_no_telemetry(self):
        # Arrange
        mock_session, _, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()

            # Act
            result = telemetry._send_batch([])

            # Assert
            assert result == False

    def test_telemetry_manager_send_batch_no_connection(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        rest_mock.rest = None

        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._write_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()

            # Act
            result = telemetry._send_batch([])

            # Assert
            TelemetryManager._write_telemetry.assert_called_once_with([])
            assert result == False

    def test_telemetry_manager_send_batch_request_true(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()
            event = {"message": {"type": "test"}}
            batch = [event]

            # Act
            result = telemetry._send_batch(batch)

            # Assert
            rest_mock.rest.request.assert_called_with(
                "/telemetry/send",
                body={"logs": batch},
                method="post",
                client=None,
                timeout=5,
            )
            assert result == True

    def test_telemetry_manager_send_batch_request_false(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import(
            request_return=False
        )
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._write_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()
            event = {"message": {"type": "test"}}
            batch = [event]

            # Act
            result = telemetry._send_batch(batch)

            # Assert
            rest_mock.rest.request.assert_called_with(
                "/telemetry/send",
                body={"logs": batch},
                method="post",
                client=None,
                timeout=5,
            )
            TelemetryManager._write_telemetry.assert_called_once_with(batch)
            assert result == False

    def test_telemetry_manager_add_log_to_batch(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import(
            request_return=False
        )
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._send_batch",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()
            telemetry.log_batch = []
            telemetry.flush_size = 2
            event = {"foo": "boo"}

            # Act
            telemetry._add_log_to_batch(event)

            # Assert
            telemetry._send_batch.assert_not_called()
            assert len(telemetry.log_batch) == 1
            assert telemetry.log_batch[0] == event

    def test_telemetry_manager_add_log_to_batch_and_clean(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import(
            request_return=False
        )
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._send_batch",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()
            telemetry.log_batch = []
            telemetry.flush_size = 1
            event = {"foo": "boo"}

            # Act
            telemetry._add_log_to_batch(event)

            # Assert
            telemetry._send_batch.assert_called_once_with([event])
            assert len(telemetry.log_batch) == 0
            assert telemetry.log_batch == []

    def test_telemetry_manager_log_telemetry(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._add_log_to_batch",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._generate_event",
            return_value={"foo": "boo"},
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager
            from snowflake.snowpark_checkpoints.utils.telemetry import _generate_event

            telemetry = TelemetryManager()

            # Act
            result = telemetry._log_telemetry(
                "event_name", "event_type", {"testing": "boo"}
            )

            # Assert
            TelemetryManager._add_log_to_batch.assert_called_once_with({"foo": "boo"})
            _generate_event.assert_called_once_with(
                "event_name",
                "event_type",
                mock_session.builder.getOrCreate().connection,
                {"testing": "boo"},
            )
            assert result == {"foo": "boo"}

    def test_telemetry_manager_log_telemetry_disable(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=False,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._add_log_to_batch",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry._generate_event"
        ) as patch_generate_event:
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()

            # Act
            result = telemetry._log_telemetry(
                "event_name", "event_type", {"testing": "boo"}
            )

            # Assert
            assert result.__eq__(None)
            telemetry._add_log_to_batch.assert_not_called()
            patch_generate_event.assert_not_called()

    def test_telemetry_manager_log_info(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._log_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.randint", return_value=1
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()

            # Act
            telemetry.log_info("event_name", {"testing": "boo"})

            # Assert
            TelemetryManager._log_telemetry.assert_called_once_with(
                "event_name", "info", {"testing": "boo"}
            )

    def test_telemetry_manager_log_error(self):
        # Arrange
        mock_session, rest_mock, mock_DIRS = mock_before_telemetry_import()
        with patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.snowflake_dirs", mock_DIRS
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._log_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._is_telemetry_enabled",
            return_value=True,
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.TelemetryManager._upload_local_telemetry",
            return_value=MagicMock(),
        ), patch(
            "snowflake.snowpark_checkpoints.utils.telemetry.Session", mock_session
        ):
            from snowflake.snowpark_checkpoints.utils.telemetry import TelemetryManager

            telemetry = TelemetryManager()

            # Act
            telemetry.log_error("event_name", {"testing": "boo"})

            # Assert
            TelemetryManager._log_telemetry.assert_called_once_with(
                "event_name", "error", {"testing": "boo"}
            )


def mock_folder_path(stat):
    folder_path = MagicMock()
    json_path = MagicMock()
    json_path.stat.return_value = stat
    json_path.is_file.return_value = True
    folder_path.glob.return_value = [json_path, json_path]

    return folder_path, json_path


def mock_before_telemetry_import(request_return=True):
    from pathlib import Path

    mock_session = MagicMock()
    rest_mock = MagicMock()
    rest_mock.rest.request.return_value = {"success": request_return}
    mock_session.builder.getOrCreate().connection = rest_mock
    mock_DIRS = MagicMock()
    mock_DIRS.user_config_path = Path("folder/")
    return mock_session, rest_mock, mock_DIRS

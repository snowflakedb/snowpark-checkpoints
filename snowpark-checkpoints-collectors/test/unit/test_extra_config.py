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
from unittest.mock import MagicMock, patch

from snowflake.snowpark_checkpoints_collector.collection_common import (
    SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR,
    CheckpointMode,
)


def test_is_checkpoint_import_error():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            _get_metadata,
        )

        enabled, _ = _get_metadata()
        assert enabled == False


def test_is_checkpoint_enabled_default():
    with patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config._get_metadata",
        return_value=(False, None),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            is_checkpoint_enabled,
        )

        actual = is_checkpoint_enabled("demo-initial-creation-checkpoint")
        assert actual


def test_is_checkpoint_enabled_no_file():
    from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
        is_checkpoint_enabled,
    )

    actual = is_checkpoint_enabled("demo-initial-creation-checkpoint")
    assert actual == True


def test_is_checkpoint_enabled_checkpoint_disabled():
    metadata_mock = MagicMock()
    metadata_mock.get_checkpoint.return_value = MagicMock(enabled=False)
    with patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config._get_metadata",
        return_value=(True, metadata_mock),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            is_checkpoint_enabled,
        )

        actual = is_checkpoint_enabled("my-checkpoint")
        assert actual == False


def test_get_checkpoint_sample_import_error():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            get_checkpoint_sample,
        )

        assert get_checkpoint_sample("checkpoint-name") == 1.0


def test_get_checkpoint_sample_import_error_with_parameter():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            get_checkpoint_sample,
        )

        assert get_checkpoint_sample("checkpoint-name", 0.5) == 0.5


def test_get_checkpoint_sample_checkpoint_value():
    metadata_mock = MagicMock()
    metadata_mock.get_checkpoint.return_value = MagicMock(sample=0.6)
    with patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config._get_metadata",
        return_value=(True, metadata_mock),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            get_checkpoint_sample,
        )

        assert get_checkpoint_sample("my-checkpoint") == 0.6


def test_get_checkpoint_mode_import_error():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            get_checkpoint_mode,
        )

        assert get_checkpoint_mode("checkpoint-name") == CheckpointMode.SCHEMA


def test_get_checkpoint_mode_import_error_with_parameter():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            get_checkpoint_mode,
        )

        assert (
            get_checkpoint_mode("checkpoint-name", CheckpointMode.SCHEMA)
            == CheckpointMode.SCHEMA
        )


def test_get_checkpoint_mode_checkpoint_value():
    metadata_mock = MagicMock()
    metadata_mock.get_checkpoint.return_value = MagicMock(mode=2)
    with patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config._get_metadata",
        return_value=(True, metadata_mock),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            get_checkpoint_mode,
        )

        assert get_checkpoint_mode("my-checkpoint") == CheckpointMode.DATAFRAME


def test_get_checkpoint_contract_file_path_env_var_set():
    os.environ[SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR] = "/mock/path"
    from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
        _get_checkpoint_contract_file_path,
    )

    assert _get_checkpoint_contract_file_path() == "/mock/path"


def test_get_checkpoint_contract_file_path_env_var_not_set():
    os.environ.pop(SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR)
    from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
        _get_checkpoint_contract_file_path,
    )

    assert _get_checkpoint_contract_file_path() == os.getcwd()


def test_set_conf_io_strategy_default():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.io_utils.io_file_manager.get_io_file_manager"
    ) as mock_get_conf_io_file_manager:
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            _set_conf_io_strategy,
        )

        # Act
        _set_conf_io_strategy()

        # Assert
        mock_get_conf_io_file_manager().set_strategy.assert_not_called()


def test_set_conf_io_strategy_custom():
    # Arrange
    from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
        _set_conf_io_strategy,
    )

    mock_file_manager = MagicMock()
    mock_file_manager.strategy = MagicMock()
    with patch(
        "snowflake.snowpark_checkpoints_configuration.io_utils.io_file_manager.get_io_file_manager"
    ) as mock_get_conf_io_file_manager, patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config.get_io_file_manager",
        return_value=mock_file_manager,
    ):
        # Act
        _set_conf_io_strategy()

        # Assert
        mock_get_conf_io_file_manager().set_strategy.assert_called_once()

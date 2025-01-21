#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
from unittest.mock import MagicMock, patch

from snowflake.snowpark_checkpoints.utils.constants import (
    SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR,
)


def test_is_checkpoint_import_error():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints.utils.extra_config import (
            _get_metadata,
        )

        enabled, _ = _get_metadata()
        assert enabled == False


def test_is_checkpoint_enabled_default():
    with patch(
        "snowflake.snowpark_checkpoints.utils.extra_config._get_metadata",
        return_value=(False, None),
    ):
        from snowflake.snowpark_checkpoints.utils.extra_config import (
            is_checkpoint_enabled,
        )

        actual = is_checkpoint_enabled("demo_initial_creation_checkpoint")
        assert actual


def test_is_checkpoint_enabled_no_checkpoint_name():
    with patch(
        "snowflake.snowpark_checkpoints.utils.extra_config._get_metadata",
        return_value=(False, None),
    ):
        from snowflake.snowpark_checkpoints.utils.extra_config import (
            is_checkpoint_enabled,
        )

        actual = is_checkpoint_enabled()
        assert actual


def test_is_checkpoint_enabled_no_file():
    from snowflake.snowpark_checkpoints.utils.extra_config import is_checkpoint_enabled

    actual = is_checkpoint_enabled("demo_initial_creation_checkpoint")
    assert actual == True


def test_is_checkpoint_enabled_checkpoint_disabled():
    metadata = MagicMock()
    metadata.get_checkpoint.return_value = MagicMock(enabled=False)
    with patch(
        "snowflake.snowpark_checkpoints.utils.extra_config._get_metadata",
        return_value=(True, metadata),
    ):
        from snowflake.snowpark_checkpoints.utils.extra_config import (
            is_checkpoint_enabled,
        )

        actual = is_checkpoint_enabled("my-checkpoint")
        assert actual == False


def test_get_checkpoint_contract_file_path_env_var_set():
    os.environ[SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR] = "/mock/path"
    from snowflake.snowpark_checkpoints.utils.extra_config import (
        _get_checkpoint_contract_file_path,
    )

    assert _get_checkpoint_contract_file_path() == "/mock/path"


def test_get_checkpoint_contract_file_path_env_var_not_set():
    os.environ.pop(SNOWFLAKE_CHECKPOINT_CONTRACT_FILE_PATH_ENV_VAR)
    from snowflake.snowpark_checkpoints.utils.extra_config import (
        _get_checkpoint_contract_file_path,
    )

    assert _get_checkpoint_contract_file_path() == os.getcwd()

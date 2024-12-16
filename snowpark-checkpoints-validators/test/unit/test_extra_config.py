#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import MagicMock, patch


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

        actual = is_checkpoint_enabled("demo-initial-creation-checkpoint")
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

    actual = is_checkpoint_enabled("demo-initial-creation-checkpoint")
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

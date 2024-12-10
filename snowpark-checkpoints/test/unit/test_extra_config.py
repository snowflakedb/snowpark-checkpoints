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
            configuration_enabled,
        )

        assert configuration_enabled == False


def test_is_checkpoint_enabled_default():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints.utils.extra_config import (
            is_checkpoint_enabled,
        )

        actual = is_checkpoint_enabled("demo-initial-creation-checkpoint")
        assert actual


def test_is_checkpoint_enabled_no_file():
    from snowflake.snowpark_checkpoints.utils.extra_config import is_checkpoint_enabled

    actual = is_checkpoint_enabled("demo-initial-creation-checkpoint")
    assert actual == True


def test_is_checkpoint_enabled_checkpoint_disabled():
    from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
        CheckpointMetadata,
    )

    metadata = MagicMock(spec=CheckpointMetadata)
    checkpoint_mock = MagicMock()
    checkpoint_mock.enabled = False
    metadata.get_checkpoint.return_value = checkpoint_mock
    with patch("snowflake.snowpark_checkpoints.utils.extra_config.metadata", metadata):
        with patch(
            "snowflake.snowpark_checkpoints.utils.extra_config.configuration_enabled",
            True,
        ):
            from snowflake.snowpark_checkpoints.utils.extra_config import (
                is_checkpoint_enabled,
            )

            actual = is_checkpoint_enabled("my-checkpoint")
            assert actual == False

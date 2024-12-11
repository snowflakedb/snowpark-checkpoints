#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import MagicMock, patch


def test_is_checkpoint_import_error():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            configuration_enabled,
        )

        assert configuration_enabled == False


def test_is_checkpoint_enabled_default():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
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
    from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
        CheckpointMetadata,
    )

    metadata = MagicMock(spec=CheckpointMetadata)
    checkpoint_mock = MagicMock()
    checkpoint_mock.enabled = False
    metadata.get_checkpoint.return_value = checkpoint_mock
    with patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config.metadata", metadata
    ):
        with patch(
            "snowflake.snowpark_checkpoints_collector.utils.extra_config.configuration_enabled",
            True,
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
    from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
        CheckpointMetadata,
    )

    metadata = MagicMock(spec=CheckpointMetadata)
    checkpoint_mock = MagicMock()
    checkpoint_mock.sample = 0.6
    metadata.get_checkpoint.return_value = checkpoint_mock
    with patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config.metadata", metadata
    ):
        with patch(
            "snowflake.snowpark_checkpoints_collector.utils.extra_config.configuration_enabled",
            True,
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

        assert get_checkpoint_mode("checkpoint-name") == 1


def test_get_checkpoint_mode_import_error_with_parameter():
    with patch(
        "snowflake.snowpark_checkpoints_configuration.checkpoint_metadata.CheckpointMetadata",
        side_effect=ImportError("Mocked exception"),
    ):
        from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
            get_checkpoint_mode,
        )

        assert get_checkpoint_mode("checkpoint-name", 1) == 1


def test_get_checkpoint_mode_checkpoint_value():
    from snowflake.snowpark_checkpoints_configuration.checkpoint_metadata import (
        CheckpointMetadata,
    )

    metadata = MagicMock(spec=CheckpointMetadata)
    checkpoint_mock = MagicMock()
    checkpoint_mock.mode = 2
    metadata.get_checkpoint.return_value = checkpoint_mock
    with patch(
        "snowflake.snowpark_checkpoints_collector.utils.extra_config.metadata", metadata
    ):
        with patch(
            "snowflake.snowpark_checkpoints_collector.utils.extra_config.configuration_enabled",
            True,
        ):
            from snowflake.snowpark_checkpoints_collector.utils.extra_config import (
                get_checkpoint_mode,
            )

            assert get_checkpoint_mode("my-checkpoint") == 2

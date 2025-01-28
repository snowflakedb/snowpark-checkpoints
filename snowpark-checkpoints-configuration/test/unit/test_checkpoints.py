#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark_checkpoints_configuration.model.checkpoints import (
    Checkpoint,
    Checkpoints,
    Pipeline,
)


def test_add_checkpoint():
    checkpoints = Checkpoints(type="Collection", pipelines=[])
    new_checkpoint = Checkpoint(
        name="checkpoint-name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )
    checkpoints.add_checkpoint(new_checkpoint)
    assert checkpoints.get_check_point("checkpoint_name") == new_checkpoint


def test_add_checkpoint_with_same_name():
    checkpoints = Checkpoints(type="Collection", pipelines=[])
    new_checkpoint = Checkpoint(
        name="checkpoint-name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )
    checkpoints.add_checkpoint(new_checkpoint)
    new_checkpoint_2 = Checkpoint(
        name="checkpoint_name",
        df="df2",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=True,
    )
    checkpoints.add_checkpoint(new_checkpoint_2)

    assert checkpoints.get_check_point("checkpoint_name") == new_checkpoint_2


def test_get_checkpoint_existing():
    new_checkpoint = Checkpoint(
        name="checkpoint-name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )
    new_pipeline = Pipeline(entry_point="entry-point", checkpoints=[new_checkpoint])
    checkpoints = Checkpoints(type="Collection", pipelines=[new_pipeline])

    expected_checkpoint = Checkpoint(
        name="checkpoint_name",
        df="df",
        mode=1,
        function="",
        file="file",
        location=1,
        enabled=False,
    )

    assert checkpoints.get_check_point("checkpoint_name") == expected_checkpoint


def test_get_checkpoint_non_existing():
    checkpoints = Checkpoints(type="Collection", pipelines=[])

    assert checkpoints.get_check_point("checkpoint-name-2") == Checkpoint(
        name="checkpoint-name-2", enabled=True
    )

#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Optional

from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext


class SnowparkCheckpointError(Exception):
    def __init__(
        self,
        message,
        job_context: Optional[SnowparkJobContext],
        checkpoint_name: str,
        data=None,
    ):
        job_name = job_context.job_name if job_context else "Unknown Job"
        super().__init__(
            f"Job: {job_name} Checkpoint: {checkpoint_name}\n{message} \n {data}"
        )
        if job_context:
            job_context._mark_fail(
                message,
                checkpoint_name,
                data,
            )


class SparkMigrationError(SnowparkCheckpointError):
    def __init__(
        self,
        message,
        job_context: Optional[SnowparkJobContext],
        checkpoint_name=None,
        data=None,
    ):
        super().__init__(message, job_context, checkpoint_name, data)


class SchemaValidationError(SnowparkCheckpointError):
    def __init__(
        self,
        message,
        job_context: Optional[SnowparkJobContext],
        checkpoint_name=None,
        data=None,
    ):
        super().__init__(message, job_context, checkpoint_name, data)

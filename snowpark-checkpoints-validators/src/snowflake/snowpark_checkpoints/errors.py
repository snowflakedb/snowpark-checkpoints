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

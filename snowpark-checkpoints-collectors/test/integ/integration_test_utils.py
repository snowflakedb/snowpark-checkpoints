#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import os
import shutil

from snowflake.snowpark_checkpoints_collector.collection_common import (
    SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME,
)


def remove_output_directory() -> None:
    current_directory_path = os.getcwd()
    output_directory_path = os.path.join(
        current_directory_path, SNOWPARK_CHECKPOINTS_OUTPUT_DIRECTORY_NAME
    )
    if os.path.exists(output_directory_path):
        shutil.rmtree(output_directory_path)

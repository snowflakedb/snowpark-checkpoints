#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os

from snowflake.snowpark import Session
from snowflake.snowpark_checkpoints_collector.collection_common import (
    DOT_PARQUET_EXTENSION,
)


STAGE_NAME = "CHECKPOINT_STAGE"
CREATE_STAGE_STATEMENT_FORMAT = "CREATE TEMPORARY STAGE IF NOT EXISTS {}"
STAGE_PATH_FORMAT = "'@{}/{}'"
PUT_PARQUET_FILES_IN_STAGE_STATEMENT_FORMAT = "PUT 'file://{}' {} AUTO_COMPRESS=FALSE"


class SnowConnection:

    """Class for manage the Snowpark Connection.

    Attributes:
        session (Snowpark.Session): the Snowpark session.

    """

    def __init__(self, session: Session = None) -> None:
        """Init SnowConnection.

        Args:
            session (Snowpark.Session): the Snowpark session.

        """
        self.session = session if session is not None else Session.builder.getOrCreate()

    def upload_to_snowflake(
        self,
        checkpoint_name: str,
        checkpoint_file_name: str,
        output_directory_path: str,
    ) -> None:
        """Upload to Snowflake's table the data collected from the DataFrame.

        Args:
            checkpoint_name (str): the name of the checkpoint.
            checkpoint_file_name (str): the checkpoint_file_name.
            output_directory_path (str): the output directory path.

        """
        stage_directory_path = STAGE_PATH_FORMAT.format(
            STAGE_NAME, checkpoint_file_name
        )
        self._create_stage()
        self._load_files_to_stage(checkpoint_file_name, output_directory_path)
        self._create_table(checkpoint_name, stage_directory_path)

    def _create_stage(self) -> None:
        create_stage_statement = CREATE_STAGE_STATEMENT_FORMAT.format(STAGE_NAME)
        self.session.sql(create_stage_statement).collect()

    def _load_files_to_stage(self, checkpoint_file_name, output_directory_path) -> None:
        parquet_files_collection = os.scandir(output_directory_path)
        stage_directory_path = STAGE_PATH_FORMAT.format(
            STAGE_NAME, checkpoint_file_name
        )
        for file in parquet_files_collection:
            is_parquet_file = file.name.endswith(DOT_PARQUET_EXTENSION)
            if is_parquet_file:
                put_statement = PUT_PARQUET_FILES_IN_STAGE_STATEMENT_FORMAT.format(
                    file.path, stage_directory_path
                )
                self.session.sql(put_statement).collect()

    def _create_table(self, checkpoint_name, stage_directory_path) -> None:
        dataframe = self.session.read.parquet(path=stage_directory_path)
        dataframe.write.save_as_table(table_name=checkpoint_name, mode="overwrite")

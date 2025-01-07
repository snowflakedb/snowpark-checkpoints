#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import glob
import os.path

from datetime import datetime
from typing import Callable, Optional

from snowflake.snowpark import Session
from snowflake.snowpark_checkpoints_collector.collection_common import (
    BACKSLASH_TOKEN,
    DOT_PARQUET_EXTENSION,
    SLASH_TOKEN,
)


STAGE_NAME = "CHECKPOINT_STAGE"
CREATE_STAGE_STATEMENT_FORMAT = "CREATE STAGE IF NOT EXISTS {}"
STAGE_PATH_FORMAT = "'@{}/{}'"
PUT_FILE_IN_STAGE_STATEMENT_FORMAT = "PUT 'file://{}' {} AUTO_COMPRESS=FALSE"


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

    def create_snowflake_table_from_local_parquet(
        self,
        table_name: str,
        input_path: str,
        stage_path: Optional[str] = None,
    ) -> None:
        """Upload to parquet files from the input path and create a table.

        Args:
            table_name (str): the name of the table to be created.
            input_path (str): the output directory path.
            stage_path: (str, optional): the stage path.

        """
        folder = f"table_files_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        stage_path = stage_path if stage_path else folder
        stage_directory_path = STAGE_PATH_FORMAT.format(STAGE_NAME, stage_path)
        self.create_temp_stage(STAGE_NAME)

        def is_parquet_file(file: str):
            return file.endswith(DOT_PARQUET_EXTENSION)

        self.load_files_to_stage(STAGE_NAME, stage_path, input_path, is_parquet_file)
        self.create_table_from_parquet(table_name, stage_directory_path)

    def create_temp_stage(self, stage_name: str) -> None:
        """Create a temp stage in Snowflake.

        Args:
            stage_name (str): the name of the stage.

        """
        create_stage_statement = CREATE_STAGE_STATEMENT_FORMAT.format(stage_name)
        self.session.sql(create_stage_statement).collect()

    def load_files_to_stage(
        self,
        stage_name: str,
        folder_name: str,
        input_directory_path: str,
        filter_func: Callable = None,
    ) -> None:
        """Load files to a stage in Snowflake.

        Args:
            stage_name (str): the name of the stage.
            folder_name (str): the folder name.
            input_directory_path (str): the output directory path.
            filter_func (Callable): the filter function to apply to the files.

        """
        target_dir = os.path.join(input_directory_path, "**", "*")
        files_collection = glob.glob(target_dir, recursive=True)

        for file in files_collection:
            if not os.path.isfile(file):
                continue
            is_match = filter_func(file) if filter_func else True
            if is_match:
                # Snowflake handle paths with slash, no matters the OS.
                normalize_file_path = file.replace(BACKSLASH_TOKEN, SLASH_TOKEN)
                new_file_path = normalize_file_path.replace(
                    input_directory_path, folder_name
                )
                stage_file_path = STAGE_PATH_FORMAT.format(stage_name, new_file_path)
                put_statement = PUT_FILE_IN_STAGE_STATEMENT_FORMAT.format(
                    normalize_file_path, stage_file_path
                )
                self.session.sql(put_statement).collect()

    def create_table_from_parquet(self, table_name, stage_directory_path) -> None:
        """Create a table from a parquet file in Snowflake.

        Args:
            table_name (str): the name of the table.
            stage_directory_path (str): the stage directory path.

        """
        dataframe = self.session.read.parquet(path=stage_directory_path)
        dataframe.write.save_as_table(table_name=table_name, mode="overwrite")

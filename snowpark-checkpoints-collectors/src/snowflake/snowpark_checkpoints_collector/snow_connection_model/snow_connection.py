#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import glob
import os.path
import time

from pathlib import Path
from typing import Callable, Optional

from snowflake.snowpark import Session
from snowflake.snowpark_checkpoints_collector.collection_common import (
    DOT_PARQUET_EXTENSION,
)


STAGE_NAME = "CHECKPOINT_STAGE"
CREATE_STAGE_STATEMENT_FORMAT = "CREATE TEMP STAGE IF NOT EXISTS {}"
REMOVE_STAGE_FOLDER_STATEMENT_FORMAT = "REMOVE {}"
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
        self.stage_id = int(time.time())

    def create_snowflake_table_from_local_parquet(
        self,
        table_name: str,
        input_path: str,
        stage_path: Optional[str] = None,
    ) -> None:
        """Upload to parquet files from the input path and create a table.

        Args:
            table_name (str): the name of the table to be created.
            input_path (str): the input directory path.
            stage_path: (str, optional): the stage path.

        """
        input_path = Path(input_path).resolve()
        folder = f"table_files_{int(time.time())}"
        stage_path = stage_path if stage_path else folder
        stage_name = f"{STAGE_NAME}_{self.stage_id}"
        stage_directory_path = STAGE_PATH_FORMAT.format(stage_name, stage_path)

        def is_parquet_file(file: str):
            return file.endswith(DOT_PARQUET_EXTENSION)

        try:
            self.create_tmp_stage(stage_name)
            self.load_files_to_stage(
                stage_name, stage_path, input_path, is_parquet_file
            )
            self.create_table_from_parquet(table_name, stage_directory_path)

        finally:
            self.session.sql(
                REMOVE_STAGE_FOLDER_STATEMENT_FORMAT.format(stage_directory_path)
            ).collect()

    def create_tmp_stage(self, stage_name: str) -> None:
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
        input_directory_path = str(Path(input_directory_path).resolve())

        def filter_files(name: str):
            if not os.path.isfile(name):
                return False
            return True if filter_func is None else filter_func(name)

        target_dir = os.path.join(input_directory_path, "**", "*")
        files_collection = glob.glob(target_dir, recursive=True)

        files = [file for file in files_collection if filter_files(file)]

        if len(files) == 0:
            raise Exception(
                f"No files were found in the input directory: {input_directory_path}"
            )

        for file in files:
            # Snowflake handle paths with slash, no matters the OS.
            normalize_file_path = Path(file).as_uri()
            new_file_path = file.replace(input_directory_path, folder_name)
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

        Raise:
            Exception: No parquet files were found in the stage

        """
        files = self.session.sql(f"LIST {stage_directory_path}").collect()
        if len(files) == 0:
            raise Exception("No parquet files were found in the stage.")
        dataframe = self.session.read.parquet(path=stage_directory_path)
        dataframe.write.save_as_table(table_name=table_name, mode="overwrite")

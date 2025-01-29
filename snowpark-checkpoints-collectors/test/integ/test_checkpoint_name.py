import os
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from snowflake.snowpark_checkpoints_collector import collect_dataframe_checkpoint
from snowflake.snowpark_checkpoints_collector.singleton import Singleton
from pathlib import Path
from snowflake.snowpark_checkpoints_collector.utils.telemetry import (
    get_telemetry_manager,
)

TELEMETRY_FOLDER = "telemetry"


@pytest.fixture
def spark_session():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def singleton():
    Singleton._instances = {}


@pytest.fixture(scope="function")
def output_path():
    folder = os.urandom(8).hex()
    directory = Path(tempfile.gettempdir()).resolve() / folder
    os.makedirs(directory)
    return str(directory)


def test_collect_dataframe_with_invalid_checkpoint_name(
    spark_session, singleton, output_path
):
    checkpoint_name = "6*invalid"
    data = []
    columns = StructType()
    pyspark_df = spark_session.createDataFrame(data=data, schema=columns)

    with pytest.raises(Exception) as ex_info:
        collect_dataframe_checkpoint(
            pyspark_df, checkpoint_name=checkpoint_name, output_path=output_path
        )
    assert (
        f"Invalid checkpoint name: {checkpoint_name}. Checkpoint names must only contain alphanumeric characters "
        f"and underscores."
    ) == str(ex_info.value)

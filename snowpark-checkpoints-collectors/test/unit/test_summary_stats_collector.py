#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from datetime import datetime
import os
import tempfile
from unittest.mock import MagicMock

import pytest

from snowflake.snowpark_checkpoints_collector.summary_stats_collector import (
    generate_parquet_for_spark_df,
)


def test_generate_parquet_for_spark_df_exception():
    spark = MagicMock()
    spark_df = MagicMock()
    spark_df.dtypes = []
    spark_df.select = MagicMock()
    spark_df = spark.createDataFrame()
    parquet_directory = os.path.join(
        tempfile.gettempdir(),
        f"test_spark_df_checkpoint_{datetime.now().strftime('%Y%m%d%H%M%S')}",
    )

    with pytest.raises(Exception, match="No parquet files were generated."):
        generate_parquet_for_spark_df(spark_df, parquet_directory)

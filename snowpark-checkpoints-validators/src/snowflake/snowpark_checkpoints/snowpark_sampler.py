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

import logging

from typing import Optional

import pandas

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.utils.constants import (
    INTEGER_TYPE_COLLECTION,
    PANDAS_LONG_TYPE,
)


LOGGER = logging.getLogger(__name__)


class SamplingStrategy:
    RANDOM_SAMPLE = 1
    LIMIT = 2


class SamplingError(Exception):
    pass


class SamplingAdapter:
    def __init__(
        self,
        job_context: Optional[SnowparkJobContext],
        sample_frac: Optional[float] = None,
        sample_number: Optional[int] = None,
        sampling_strategy: SamplingStrategy = SamplingStrategy.RANDOM_SAMPLE,
    ):
        self.pandas_sample_args = []
        self.job_context = job_context
        if sample_frac and not (0 <= sample_frac <= 1):
            raise ValueError(
                f"'sample_size' value {sample_frac} is out of range (0 <= sample_size <= 1)"
            )

        self.sample_frac = sample_frac
        self.sample_number = sample_number
        self.sampling_strategy = sampling_strategy

    def process_args(self, input_args):
        # create the intermediate pandas
        # data frame for the test data
        LOGGER.info("Processing %s input argument(s) for sampling", len(input_args))
        for arg in input_args:
            if isinstance(arg, SnowparkDataFrame):
                df_count = arg.count()
                if df_count == 0:
                    raise SamplingError(
                        "Input DataFrame is empty. Cannot sample from an empty DataFrame."
                    )

                LOGGER.info("Sampling a Snowpark DataFrame with %s rows", df_count)
                if self.sampling_strategy == SamplingStrategy.RANDOM_SAMPLE:
                    if self.sample_frac:
                        LOGGER.info(
                            "Applying random sampling with fraction %s",
                            self.sample_frac,
                        )
                        df_sample = to_pandas(arg.sample(frac=self.sample_frac))
                    else:
                        LOGGER.info(
                            "Applying random sampling with size %s", self.sample_number
                        )
                        df_sample = to_pandas(arg.sample(n=self.sample_number))
                else:
                    LOGGER.info(
                        "Applying limit sampling with size %s", self.sample_number
                    )
                    df_sample = to_pandas(arg.limit(self.sample_number))

                LOGGER.info(
                    "Successfully sampled the DataFrame. Resulting DataFrame shape: %s",
                    df_sample.shape,
                )
                self.pandas_sample_args.append(df_sample)
            else:
                LOGGER.debug(
                    "Argument is not a Snowpark DataFrame. No sampling is applied."
                )
                self.pandas_sample_args.append(arg)

    def get_sampled_pandas_args(self):
        return self.pandas_sample_args

    def get_sampled_snowpark_args(self):
        if self.job_context is None:
            raise SamplingError("Need a job context to compare with Spark")
        snowpark_sample_args = []
        for arg in self.pandas_sample_args:
            if isinstance(arg, pandas.DataFrame):
                snowpark_df = self.job_context.snowpark_session.create_dataframe(arg)
                snowpark_sample_args.append(snowpark_df)
            else:
                snowpark_sample_args.append(arg)
        return snowpark_sample_args

    def get_sampled_spark_args(self):
        if self.job_context is None:
            raise SamplingError("Need a job context to compare with Spark")
        pyspark_sample_args = []
        for arg in self.pandas_sample_args:
            if isinstance(arg, pandas.DataFrame):
                pyspark_df = self.job_context.spark_session.createDataFrame(arg)
                pyspark_sample_args.append(pyspark_df)
            else:
                pyspark_sample_args.append(arg)
        return pyspark_sample_args


def to_pandas(sampled_df: SnowparkDataFrame) -> pandas.DataFrame:
    """Convert a Snowpark DataFrame to a Pandas DataFrame, handling missing values and type conversions."""
    LOGGER.debug("Converting Snowpark DataFrame to Pandas DataFrame")
    pandas_df = sampled_df.toPandas()
    pandas_df = normalize_missing_values_pandas(pandas_df)
    return pandas_df


def normalize_missing_values_pandas(df: pandas.DataFrame) -> pandas.DataFrame:
    """Normalize missing values in a Pandas DataFrame to ensure consistent handling of NA values."""
    fill_values = {}
    for col, dtype in df.dtypes.items():
        if dtype in INTEGER_TYPE_COLLECTION or str(dtype) in PANDAS_LONG_TYPE:
            fill_values[col] = 0
        elif dtype is float or dtype == "float64":
            fill_values[col] = 0.0
        elif dtype is bool or dtype == "bool" or dtype == "boolean":
            fill_values[col] = False
        elif dtype is object or dtype == "object" or dtype is str:
            fill_values[col] = ""
    return df.fillna(value=fill_values)

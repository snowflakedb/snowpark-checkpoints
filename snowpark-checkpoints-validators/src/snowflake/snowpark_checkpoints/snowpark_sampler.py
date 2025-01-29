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

import pandas

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext


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
        for arg in input_args:
            if isinstance(arg, SnowparkDataFrame):
                if arg.count() == 0:
                    raise SamplingError(
                        "Input DataFrame is empty. Cannot sample from an empty DataFrame."
                    )

                if self.sampling_strategy == SamplingStrategy.RANDOM_SAMPLE:
                    if self.sample_frac:
                        df_sample = arg.sample(frac=self.sample_frac).to_pandas()
                    else:
                        df_sample = arg.sample(n=self.sample_number).to_pandas()
                else:
                    df_sample = arg.limit(self.sample_number).to_pandas()

                self.pandas_sample_args.append(df_sample)
            else:
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

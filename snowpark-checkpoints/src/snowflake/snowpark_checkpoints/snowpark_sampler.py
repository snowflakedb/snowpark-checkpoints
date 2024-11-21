from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark import DataFrame as SnowparkDataFrame
import pandas


class SamplingStrategy:
    RANDOM_SAMPLE = 1
    LIMIT = 2


class SamplingError(Exception):
    pass


class SamplingAdapter:
    def __init__(
        self,
        job_context: SnowparkJobContext,
        sample_size: float = 1,
        sampling_strategy: SamplingStrategy = SamplingStrategy.RANDOM_SAMPLE,
    ):
        self.pandas_sample_args = []
        self.job_context = job_context
        self.sample_size = sample_size
        self.sampling_strategy = sampling_strategy

    def process_args(self, input_args):
        # create the intermediate pandas
        # data frame for the test data
        for arg in input_args:
            if isinstance(arg, SnowparkDataFrame):
                if self.sampling_strategy == SamplingStrategy.RANDOM_SAMPLE:
                    df_sample = arg.sample(self.sample_size).to_pandas()
                else:
                    df_sample = arg.limit(self.sample_size).to_pandas()
                self.pandas_sample_args.append(df_sample)
            else:
                self.pandas_sample_args.append(arg)

    def get_sampled_pandas_args(self):
        return self.pandas_sample_args

    def get_sampled_snowpark_args(self):
        if self.job_context == None:
            raise SamplingError("Need a job context to compare with spark")
        snowpark_sample_args = []
        for arg in self.pandas_sample_args:
            if isinstance(arg, pandas.DataFrame):
                snowpark_df = self.job_context.snowpark_session.create_dataframe(arg)
                snowpark_sample_args.append(snowpark_df)
            else:
                snowpark_sample_args.append(arg)
        return snowpark_sample_args

    def get_sampled_spark_args(self):
        if self.job_context == None:
            raise SamplingError("Need a job context to compare with spark")
        pyspark_sample_args = []
        for arg in self.pandas_sample_args:
            if isinstance(arg, pandas.DataFrame):
                pyspark_df = self.job_context.spark_session.createDataFrame(arg)
                pyspark_sample_args.append(pyspark_df)
            else:
                pyspark_sample_args.append(arg)
        return pyspark_sample_args

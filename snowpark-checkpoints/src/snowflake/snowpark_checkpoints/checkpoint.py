# Wrapper around pandera which logs to snowflake
import os
from typing import Any, Dict, Optional
from pandera import DataFrameSchema
import pandera as pa
import numpy as np
import pandas
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import SamplingAdapter, SamplingStrategy
from snowflake.snowpark import DataFrame as SnowparkDataFrame

def check_pandera_df_schema_file(df:SnowparkDataFrame, 
                            checkpoint_name:str = None,
                            job_context:SnowparkJobContext = None,
                            sample: Optional[int] = 100,
                            sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
                                 file_path: str=None):
    file_name = f"snowpark-{checkpoint_name}-schema.json"
    file_name = file_path if file_path else file_name

    if not os.path.isfile(file_name):
        raise Exception(f"checkpoint file {file_name} doesn't exist")

    schema_file = open(file_name, "r")
    schema = pa.DataFrameSchema.from_json(schema_file)
    check_pandera_df_schema(df, schema, job_context, checkpoint_name, sample, sampling_strategy)
                            
def check_pandera_df_schema(df:SnowparkDataFrame, 
                            pandera_schema:DataFrameSchema,
                            job_context:SnowparkJobContext = None,
                            checkpoint_name:str = None,
                            sample: Optional[int] = 100,
                            sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE):
    sampler = SamplingAdapter(job_context, sample, sampling_strategy)
    sampler.process_args([df])
    
    # fix up the column casing
    pandera_schema_upper = pandera_schema
    new_columns:Dict[Any, Any] = {} 
    for col in pandera_schema.columns:
        new_columns[col.upper()] = pandera_schema.columns[col]
    pandera_schema_upper= pandera_schema_upper.remove_columns(pandera_schema.columns)
    pandera_schema_upper= pandera_schema_upper.add_columns(new_columns)
    sample_df = sampler.get_sampled_pandas_args()[0]
    sample_df.index = np.ones(sample_df.count().iloc[0])
    # Raises SchemaError on validation issues
    try:
        pandera_schema_upper.validate(sample_df)
        if job_context is not None:
            job_context.mark_pass(checkpoint_name)
    except Exception as pandera_ex:
        raise SchemaValidationError("Snowpark output schema validation error", job_context, checkpoint_name, pandera_ex)

def check_pandera_output_schema(pandera_schema:DataFrameSchema,
                            sample: Optional[int] = 100,
                            sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
                            job_context:SnowparkJobContext = None,
                            check_name: Optional[str] = None):
    def check_output_with_pandera_decorator(snowpark_fn):
        checkpoint_name = check_name
        if check_name == None:
            checkpoint_name = snowpark_fn.__name__
        def wrapper(*args,**kwargs):
            # Run the sampled data in snowpark
            snowpark_results = snowpark_fn(*args, **kwargs)
            sampler = SamplingAdapter(job_context, sample, sampling_strategy)
            sampler.process_args([snowpark_results])
            pandas_sample_args = sampler.get_sampled_pandas_args()
            
            # Raises SchemaError on validation issues
            try:
                pandera_schema.validate(pandas_sample_args[0])
                if job_context is not None:
                    job_context.mark_pass(checkpoint_name)
            except Exception as pandera_ex:
                raise SchemaValidationError("Snowpark output schema validation error", job_context, checkpoint_name, pandera_ex)
            return snowpark_results
        return wrapper
    return check_output_with_pandera_decorator

def check_pandera_input_schema(pandera_schema:DataFrameSchema,
                            sample: Optional[int] = 100,
                            sampling_strategy: Optional[SamplingStrategy] = SamplingStrategy.RANDOM_SAMPLE,
                            job_context:SnowparkJobContext = None,
                            check_name: Optional[str] = None):
    def check_input_with_pandera_decorator(snowpark_fn):
        checkpoint_name = check_name
        if check_name == None:
            checkpoint_name = snowpark_fn.__name__
        def wrapper(*args,**kwargs):
            # Run the sampled data in snowpark
            sampler = SamplingAdapter(job_context, sample, sampling_strategy)
            sampler.process_args(args)
            pandas_sample_args = sampler.get_sampled_pandas_args()
            
            # Raises SchemaError on validation issues
            for arg in pandas_sample_args:
                if isinstance(arg, pandas.DataFrame):
                    try:
                        pandera_schema.validate(arg)
                        if job_context is not None:
                            job_context.mark_pass(checkpoint_name)
                    except Exception as pandera_ex:
                        raise SchemaValidationError("Snowpark schema input validation error", job_context, checkpoint_name, pandera_ex)
            return snowpark_fn(*args, **kwargs)
        return wrapper
    return check_input_with_pandera_decorator


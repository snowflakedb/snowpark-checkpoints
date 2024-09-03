# Wrapper around pandera which logs to snowflake
from typing import Optional
from pandera import DataFrameSchema
import pandas
from snowflake.snowpark_checkpoints.errors import SchemaValidationError
from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext
from snowflake.snowpark_checkpoints.snowpark_sampler import SamplingAdapter, SamplingStrategy

def check_output_with_pandera(pandera_schema:DataFrameSchema,
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
                job_context.mark_pass(checkpoint_name)
            except Exception as pandera_ex:
                raise SchemaValidationError("Snowpark output schema validation error", job_context, checkpoint_name, pandera_ex)
            return snowpark_results
        return wrapper
    return check_output_with_pandera_decorator

def check_input_with_pandera(pandera_schema:DataFrameSchema,
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
                        job_context.mark_pass(checkpoint_name)
                    except Exception as pandera_ex:
                        raise SchemaValidationError("Snowpark schema input validation error", job_context, checkpoint_name, pandera_ex)
            return snowpark_fn(*args, **kwargs)
        return wrapper
    return check_input_with_pandera_decorator


from snowflake.snowpark_checkpoints.job_context import SnowparkJobContext

class SnowparkCheckpointError(Exception):
    def __init__(self, 
            message, 
            job_context:SnowparkJobContext,
            checkpoint_name: str,
            data = None):
        super().__init__(f"Job: {job_context.job_name} Checkpoint: {checkpoint_name}\n{message}")
        job_context.mark_fail(message, checkpoint_name, data)           

class SparkMigrationError(SnowparkCheckpointError):
    def __init__(self, 
                message, 
                job_context:SnowparkJobContext,
                checkpoint_name = None, 
                data = None):
        super().__init__(message, job_context, checkpoint_name, data)
        
class SchemaValidationError(SnowparkCheckpointError):
    def __init__(self, 
                message, 
                job_context:SnowparkJobContext,
                checkpoint_name = None, 
                data = None):
        super().__init__(message, job_context, checkpoint_name, data)
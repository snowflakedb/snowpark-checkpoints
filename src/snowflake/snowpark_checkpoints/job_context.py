

from typing import Optional
from pyspark.sql import SparkSession
import snowflake.snowpark as snowpark

class SnowparkJobContext:
    
    def __init__(self,
          snowpark_session:snowpark.Session,
          spark_session:SparkSession = None,
          job_name:Optional[str] = None,
          log_results:Optional[bool]=True):
     self.log_results = log_results
     self.job_name = job_name
     self.spark_session = spark_session or SparkSession.builder.getOrCreate()
     self.snowpark_session = snowpark_session

     
     
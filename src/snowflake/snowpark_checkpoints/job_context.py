

from datetime import datetime
import pandas as pd
from typing import Optional
from pyspark.sql import SparkSession
from snowflake.snowpark import Session

class SnowparkJobContext:
    
    def __init__(self,
          snowpark_session:Session,
          spark_session:SparkSession = None,
          job_name:Optional[str] = None,
          log_results:Optional[bool]=True):
     self.log_results = log_results
     self.job_name = job_name
     self.spark_session = spark_session or SparkSession.builder.getOrCreate()
     self.snowpark_session = snowpark_session

     
    def mark_fail(self, message, checkpoint_name, data):
        if self.log_results:
            session = self.snowpark_session
            df = pd.DataFrame({
                "DATE": [datetime.now()],
                "JOB": [self.job_name],
                "STATUS": ["fail"],
                "CHECKPOINT": [checkpoint_name],
                "MESSAGE": [message],
                "DATA": [f"{data}"],
            })
            report_df = session.createDataFrame(df)
            report_df.write.mode("append").save_as_table("SNOWPARK_CHECKPOINTS_REPORT") 
            
    def mark_pass(self, checkpoint_name):
        if self.log_results:
            session = self.snowpark_session
            df = pd.DataFrame({
                "DATE": [datetime.now()],
                "JOB": [self.job_name],
                "STATUS": ["pass"],
                "CHECKPOINT": [checkpoint_name],
                "MESSAGE": [""],
                "DATA": [f""],
            })
            report_df = session.createDataFrame(df)
            report_df.write.mode("append").save_as_table("SNOWPARK_CHECKPOINTS_REPORT") 
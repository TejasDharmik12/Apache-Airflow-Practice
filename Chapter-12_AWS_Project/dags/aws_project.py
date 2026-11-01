import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),'..'))
from utils.bronze_layer import BronzeLayer
from utils.silver_layer import SilverLayer
from utils.gold_layer import GoldLayer

from airflow.sdk import dag,task
from datetime import timedelta
@dag(schedule= '@daily', is_paused_upon_creation = False)
def aws_project():
    @task.python(retries = 3,retry_delay = timedelta(seconds=5))
    def extract_load():
        urls = ["https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv, https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/passengers.csv","https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/airports.csv"]
        obj = BronzeLayer()
        for url in urls:
            fetched_data = obj.ingest_data_api(url)
            obj.put_data_s3("tejasS3",f"bronze/{url.split('/')[-1]}",fetched_data)
        return folder_name
            
    @task.python(retries = 3,retry_delay = timedelta(seconds=5))
    def transform_load_s3(ti):
        last_load_date = ti.xcom_pull(task_ids='extract_load_s3',key = 'return_value')
        
        obj = SilverLayer()
        # Triggering the spark job in AWS Glue to transform the data and load it to s3
        job_run_id = obj.trigger_spark_job("silver_layer",last_load_date)
        print("Glue job triggered with Job ID: {job_run_id}")
        
        # in parquet format load in s3
    @task.python(retries = 3,retry_delay = timedelta(seconds=5))
    def transform_load_s3_parquet(ti):
        last_load_date = ti.xcom_pull(task_ids='extract_load_s3',key = 'return_value')
        
        obj = SilverLayer()
        # Triggering the spark job in AWS Glue to transform the data and load it to s3
        job_run_id = obj.trigger_spark_job("silver_layer_athena",last_load_date)
        print("Glue job triggered with Job ID: {job_run_id}")    
        
        
    # Task to trigger glue crawler
    @task.python(retries = 3,retry_delay = timedelta(seconds=5))
    def trigger_crawler(ti):
        obj = SilverLayer()
        crawler_response = obj.trigger_crawler("crawler_silver")
        print(crawler_response)
        
        
        
    @task.python(retries = 3,retry_delay = timedelta(seconds=5))
    def trigger_databricks_job():
        obj = GoldLayer()
        response = obj.trigger_databricks_job(job_id = 942142703854946)
        print(response)
        
    extract_load()>> [transform_load_s3(),transform_load_s3_parquet()] >> trigger_crawler() >> trigger_databricks_job()
    
aws_project = aws_project()
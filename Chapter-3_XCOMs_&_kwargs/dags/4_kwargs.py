from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator
from datetime import timedelta

@dag
def kwargs_dag():
    
    @task.python(retries=3, retry_delay=timedelta(seconds = 5))
    def fetch_data(**kwargs):
        ti = kwargs['ti']
        data = {"name":"Airflow","version":"3.0"}
        # Pushing data to XCOM manually
        ti.xcom_push(key="fetched_data",value="data")
        return data
    
        
    
    @task.python
    def process_data(**kwargs):
        # Pull the data manually
        ti = kwargs['ti']
        pulled_data = ti.xcom_pull(key="fetched_data",task_ids="fetch_data")
        processed_data  = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)
        
    @task.bash
    def bash_task(**kwargs):
        return "echo 'This is Bash Task! the fetched data is {{ti.x_com_pull(key='fetched_data',task_ids='fetch_data')}}'"
    
    
    fetch_data() >> process_data() >> bash_task
    
dag = kwargs_dag()
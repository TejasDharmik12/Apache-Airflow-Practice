from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator
import pendulum
@dag(
    start_date = pendulum.datetime(year=2026,month=10,day=15,tz = "America/Halifax"),
    schedule = "52 10 * * *",
    catchup = True,
    is_paused_upon_creation = False
)
def schedule_dag():
    
    @task.python
    def fetch_data(ti):
        data = {"name":"Airflow","version":"3.0"}
        # Pushing data to XCOM manually
        t1.xcom_push(key="fetched_data",value="data")
        return data
    
        
    
    @task.python
    def process_data(ti):
        # Pull the data manually
        pulled_data = t1.xcom_pull(key="fetched_data",task_ids="fetch_data")
        processed_data  = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)
        
    bash_task = BashOperator(
        task_id = "bash_task",
        bash_command = "echo 'This is Bash Task'!"
    )
    
    fetch_data() >> process_data() >> bash_task
    
dag = xcoms_manual() 
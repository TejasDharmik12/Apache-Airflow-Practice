from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator

# No need of ti -> task instance required
# No need to push it  as we have return data command in the function

@dag
def xcoms_auto():
    @task.python
    # By default it is writing do_xcom_push - True
    def fetch_data(do_xcom_push: bool = True) -> dict:
        data = {"name":"Airflow","version":"3.0"}
        return data
    
    @task.python
    def process_data(pulled_data: dict):
        processed_data  = f"Processed {pulled_data['name']} version {pulled_data['version']}"
        print(processed_data)
        
    bash_task = BashOperator(
        task_id = "bash_task",
        bash_command = "echo 'This is Bash Task'!"
    )
    
    pulled_data = fetch_data()
    process_data(pulled_data) >> bash_task
    
dag = xcoms_auto()
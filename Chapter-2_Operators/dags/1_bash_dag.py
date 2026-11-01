from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator

@dag(dag_id = "bash_dag")
def bash_dag():
    # Modern Approach
    @task.bash
    def first_task():
        return "echo 'Hello'"
    
    # Traditional approach it have already an instance
    second_task = BashOperator(
        task_id = "second_task",
        bash_command = "echo 'This is second'")
    
    # Define task dependencies
    first_task_ins = first_task()
    first_task_ins >> second_task

# creating instance of dag
bash_dag_ins = bash_dag()
    
    
    
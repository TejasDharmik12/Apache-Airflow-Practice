from airflow.sdk import dag,task
from airflow.operators.python import PythonOperator
from airflow import DAG
# Python operator usage in dag operator
def first_task_func():
    return "Hello"

def second_task_func():
    return "Second"


with DAG(dag_id = "python_context_dag") as dag:
    first_task = PythonOperator(
            task_id = "first_task",
            python_callable = first_task_func
        )
        
    second_task = PythonOperator(
        task_id = "second_task",
        python_callable = second_task_func
    )
    
    first_task >> second_task
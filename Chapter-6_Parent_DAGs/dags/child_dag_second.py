from airflow.sdk import dag,task
import os

@dag(is_paused_upon_creation = False)  #child dag must not be blocked on running parent dag
def child_dag_second_dag():
    @task
    def task_pre():
        print("task A")
        
    @task
    def task_write():
        os.makedirs("/tmp/data",exist_ok=True)
        
        with open("/tmp/data/output_first.txt","w") as f:
            f.write("This is the first file")
        
    task_pre() >> task_write()
    
child_dag_second_dag = child_dag_second_dag()
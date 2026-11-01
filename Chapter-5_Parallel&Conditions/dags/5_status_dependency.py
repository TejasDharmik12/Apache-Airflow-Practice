from airflow.sdk import dag,task
from airflow.utils.trigger_rule import TriggerRule

@dag
def status_dependency():
    @task.python
    def task_a():
        print("Executing task A")
        return "Task A completed"
    
    @task.python
    def task_b():
        raise Exception("b fails")
        
    @task.python
    def task_c():
        print("Executing task A")
        return "Task A completed"
        
    @task.python(trigger_rule = TriggerRule.ALL_DONE)
    def task_d():
        print("Executing task A")
        return "Task A completed"
    
    task_a = task_a()
    task_b = task_b()
    task_c = task_c()
    task_d = task_d()
    
    task_a >> [task_b >> task_c] >> task_d

status_dependency = status_dependency()
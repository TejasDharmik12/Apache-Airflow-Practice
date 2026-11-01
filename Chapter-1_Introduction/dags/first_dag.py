from apache.sdk import dag,task

@dag(dag_id = "first_dag")
def first_dag():
    @task(task_id = "task1")
    def task1():
        print("This is task 1")
    @task(task_id = "task2")
    def task2():
        print("This is task 2")

    t1 = task1()
    t2 = task2()
    # Task 1 will run before task 2
    t1 >> t2
    
# to run the DAG, we need to create an instance of it
first_dag_instance = first_dag()
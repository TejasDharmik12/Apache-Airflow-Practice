from airflow.sdk import dag,task

@dag
def parallel_dag():
    @task
    def task_1():
        print("task_1")
    
    @task
    def task_2():
        print("task_1")

    @task
    def task_3():
        print("task_1")
    @task
    def task_4():
        print("task_1")
        
    t1 = task_1()
    t2 = task_2()
    t3 = task_3()
    t4 = task_4()
    [t1,t2,t3] >>t4
            
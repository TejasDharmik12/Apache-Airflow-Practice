from airflow.sdk import dag,task,task_group

@dag
def parallel_dag_rec():
    
    @task.bash
    def task_bash():
        return "echo 'Hello'"
    
    @task_group
    def task_fetch_group():
        @task.python
        def fetch_api():
            data = {"type":"api", "data": ["data1","data2","data3"]}
            return data
        
        @task.python
        def fetch_db():
            data = {"type":"db", "data": ["data4","data5","data6"]}
            return data
        
        @task.python
        def fetch_s3():
            data = {"type":"s3", "data": ["data7","data8","data9"]}
            return data
        
        # task group dependencies and [] for parallel tasks
        [fetch_api() >> fetch_db() >> fetch_s3()]
    
    @task.python
    def process_data(ti):
        api_data = ti.xcom_pull(task_ids='fetch_api',key = 'return_value')
        db_data = ti.xcom_pull(task_ids='fetch_db',key = 'return_value')
        s3_data = ti.xcom_pull(task_ids='fetch_s3',key = 'return_value')
        
        processed_data = api_data['data'] + db_data['data'] + s3_data['data']
        return processed_data
        
    @task.branch
    def load_data_branch(ti):
        processed_data = ti.xcom_pull(task_ids='process_data',key = 'return_value')
        if len(processed_data)>10:
            return 's3_load' #go to that task name
        else:
            return 'glue_load'
    
    @task.python
    def s3_load(ti):
        data_to_load = ti.xcom_pull(task_ids='process_data',key = 'return_value')
        print("Loading data to s3", data_to_load)
        
        
    @task.python
    def glue_load(ti):
        data_to_load = ti.xcom_pull(task_ids='process_data',key = 'return_value')
        print("Loading data to glue", data_to_load)
            
    task_bash = task_bash()
    load_data_branch = load_data_branch()
    s3_load = s3_load()
    glue_load = glue_load()
    
    # Cleaner
    task_bash >> task_fetch_group() >> process_data() >> load_data_branch >> [s3_load,glue_load]
    
parallel_dag_rec = parallel_dag_rec()
from airflow.sdk import dag,task
import requests
import pandas as pd
import os 
from sqlalchemy import create_engine, text
from airflow.providers.common.sql.operators.sql import SQLExecutionQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
@dag
def etl_pipeline():
    
    @task
    def timestamp():
        from datetime import datetime
        return datetime.now().isoformat()
    
    @task
    def extract(ti):
        # fetching timestamp from previous task
        timestamp = ti.xcom_pull(task_ids="timestamp",key="return_value")
        
        url = "http://fastapi:8000/fetch_data"
        response = requests.get(url)
        data = response.json().get("data",[])
        
        # create the folder
        os.makedirs("/tmp/raw",exist_ok=True)
        
        # Writing the data in staging layer ("/tmp/raw/data.csv")
        with open(f"/tmp/raw/data_{timestamp}.csv","w") as f:
            f.write("id,name,age\n") #writing header
            for item in data:
                f.write(f"{item['id']},{item['name']},{item['age']}\n")
        return "Data is extracted and stored in staging layer"            
        
    
    @task
    def transform(ti):
        timestamp = ti.xcom_pull(task_ids=timestamp,key="return_value")
        # Reading data from the staging layer
        df = pd.read_csv(f"/tmp/raw/data_{timestamp}.csv")
        
        # Transforming data
        df["age_group"] = df["age"].apply(lambda x: "Young" if x<30 else "Adult")
        
        # Create the transformed folder
        os.makedirs("/tmp/transformed",exist_ok=True)
        
        # Writing transformed data to transformed layer
        df.to_csv(f"/tmp/transformed/data_transformed_{timestamp}.csv",index=False)
    

    @task
    def create_tables():
        query = """
            CREATE TABLE IF NOT EXISTS employees(
                id INT,
                name VARCHAR(255),
                age INT,
                age_group VARCHAR(50)
            );
        """
        # create the connection
        conn = create_engine("postgresql://airflow:airflow@postgresql:5432/airflow").connect()
        
        with conn.begin() as transaction:
            try:
                conn.execute(text(query))
            except:
                transaction.rollback()
                raise e
            else:
                transaction.commit()
    
    @task
    def load(ti):
        timestamp = ti.xcom_pull(task_ids=timestamp,key="return_value")
        
        # Reading transformed data
        df = pd.read_csv(f"/tmp/transformed/data_transformed_{timestamp}.csv")
        
        # Load data in postgresql
        engine = create_engine("postgresql://airflow:airflow@postgresql:5432/airflow")
        df.to_sql("employees", con=engine,if_exists = "append", index=False)
        
        engine.dispose()
        
    # Another method to create the table and connection in postgresql using SQLOperators 
    create_new_table = SQLExecutionQueryOperator(
        task_id = "create_students_table",
        conn_id = "my_postgresql",
        sql = """
            CREATE TABLE IF NOT EXISTS students(
                id INT,
                name VARCHAR(255),
                age INT,
                age_group VARCHAR(50)
            );
        """
    )
        
    # Write the transformed data in a new table using PostgresHook connect external outside python
    @task
    def write_to_new_table(ti):
        timestamp = ti.xcom_pull(task_ids=timestamp,key="return_value")
        hook = PostgresHook(postgres_conn_id = "my_postgresql")
        hook.copy_expert(
            sql = """
            COPY students(id,name,age,age_group)
            from STDIN WITH CSV HEADER
            """,
            filename=f"/tmp/transformed/data_transformed_{timestamp}.csv"
        )
    
    create_tables = create_tables()
    # define the task dependencies
    timestamp()>>extract()>>transform()>>[create_tables,create_new_table] 
    create_tables >> load()
    create_new_table >> write_to_new_table()
        
etl_pipeline = etl_pipeline()
    
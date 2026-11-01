from airflow.sdk import dag,task
from airflow.timetables.interval import CronDataIntervalTimetable
import pendulum
@dag(
    schedule = CronDataIntervalTimetable("0 0 * * *",timezone="America/Halifax"),
    start_date=pendulum.datetime(year=2026, month=1,  day=15, tz="America/Halifax")
)
def incremental_load():
    @task.python
    def extract_data(**kwargs):
        # Extracting dates
        from_date = kwargs['data_interval_start']
        to_date = kwargs['data_interval_end']
        
        print(f"Extracting data from {from_date} to {to_date}")
        print(f"Select * from source_table where date >= '{from_date}' and date < '{to_date}'")
        
    @task.bash
    def load_data():
        return "echo Data loaded from {{data_interval_start | ds}} to {{data_interval_end | ds}}"
    

    extract_data()>>load_data()

dag = incremental_load()
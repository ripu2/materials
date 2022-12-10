from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file_3 = Dataset("/tmp/my_file_3.txt")

with DAG(
    dag_id="producer1",
    schedule="@daily",
    start_date= datetime(2022, 11, 12),
    catchup=False
): 
 
    @task(outlets=[my_file_3]) # outlet defines that below function performs action on my_file
    def update_dataset():
        with open(my_file_3.uri, "a+") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    update_dataset()
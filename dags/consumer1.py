from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

my_file_3 = Dataset("/tmp/my_file_3.txt")
my_file_4 = Dataset("/tmp/my_file_4.txt")


with DAG(
    dag_id="consumer1",
    schedule=[my_file_3, my_file_4],
    start_date= datetime(2022, 11, 12),
    catchup=False
):
    @task
    def read_dataset():
        with open(my_file_3.uri, "r") as f:
            print(f.read())
    # @task
    # def read_dataset2():
    #     with open(my_file_4.uri, "r") as f:
    #         print(f.read())


    read_dataset()
from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")
my_file2 = Dataset("/tmp/my_file_2.txt")
my_file_3 = Dataset("/tmp/my_file_3.txt")
my_file_4 = Dataset("/tmp/my_file_4.txt")


with DAG(
    dag_id="consume",
    schedule=[my_file, my_file2],
    start_date= datetime(2022, 11, 12),
    catchup=False
):

    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    @task(outlets=[my_file_3]) # outlet defines that below function performs action on my_file
    def update_dataset():
        with open(my_file_3.uri, "a+") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    @task(outlets=[my_file_4]) # outlet defines that below function performs action on my_file
    def update_dataset2():
        with open(my_file_4.uri, "a+") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))



    read_dataset() >> update_dataset() >> update_dataset2()
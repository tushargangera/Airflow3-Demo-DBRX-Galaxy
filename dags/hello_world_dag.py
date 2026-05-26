from airflow.sdk.dag import dag
from airflow.sdk.task import task
from datetime import datetime


@dag(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "hello_world"],
)
def hello_world():
    @task
    def say_hello():
        print("Hello, Airflow 3!")

    say_hello()


dag = hello_world()

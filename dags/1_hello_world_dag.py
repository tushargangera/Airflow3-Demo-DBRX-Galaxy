from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="1_hello_world_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "hello_world"],
)
def hello_world():
    @task
    def say_hello() -> str:
        return "Hello, Airflow 3!"

    say_hello()


dag = hello_world()

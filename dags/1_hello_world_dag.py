from airflow.sdk import dag, task # type: ignore
from datetime import datetime, timedelta


default_args = {
    "email": ["airflow-demo@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="1_hello_world_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 5 * * *",
    catchup=False,
    default_args=default_args,
    tags=["example", "hello_world"],
)
def hello_world():
    @task
    def say_hello() -> str:
        return "Hello, Airflow 3!"

    say_hello()


dag = hello_world()

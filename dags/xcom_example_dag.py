from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="xcom_example_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "xcom"],
)
def xcom_example():
    @task
    def push_value():
        return "airflow-3-xcom-value"

    @task
    def pull_value(value: str):
        print(f"Pulled from XCom: {value}")

    pull_value(push_value())


dag = xcom_example()

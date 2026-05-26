from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="2_xcom_example_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "xcom"],
)
def xcom_example():
    @task
    def push_value(ti=None) -> str:
        # push an explicit key/value pair to XCom
        ti.xcom_push(key="my_key", value="airflow-3-xcom-value")
        return "airflow-3-return-value"

    @task
    def pull_value(value: str, ti=None) -> None:
        # pull the same key from XCom, and also receive the returned value
        pulled = ti.xcom_pull(task_ids="push_value", key="my_key")
        print(f"Pulled from XCom (key=my_key): {pulled}")
        print(f"Pulled from task return: {value}")

    pull_value(push_value())


dag = xcom_example()

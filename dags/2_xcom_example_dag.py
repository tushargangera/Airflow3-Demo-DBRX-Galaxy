from airflow.sdk.dag import dag
from airflow.sdk.task import task
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
    def push_value(ti=None):
        # push an explicit key/value pair to XCom
        ti.xcom_push(key="my_key", value="airflow-3-xcom-value")
        return "airflow-3-xcom-value"

    @task
    def pull_value(dummy_value: str, ti=None):
        # pull the value using the same key from the push task
        pulled = ti.xcom_pull(task_ids="push_value", key="my_key")
        print(f"Pulled from XCom (key=my_key): {pulled}")

    pull_value(push_value())


dag = xcom_example()

from airflow.sdk import dag, task # type: ignore
from datetime import datetime, timedelta


default_args = {
    "email": ["tushar.gangera@oracle.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="2_xcom_example_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * 1-5", # At 08:00 on every day-of-week from Monday through Friday
    catchup=False,
    default_args=default_args,
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

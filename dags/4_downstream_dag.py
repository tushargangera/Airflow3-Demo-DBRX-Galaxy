from airflow.sdk import dag, task
from airflow.models import Variable
from datetime import datetime


@dag(
    dag_id="4
    _downstream_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "dag_dependency"],
)
def upstream_dag():
    @task
    def set_shared_variable():
        Variable.set("dag_dependency_message", "Hello from upstream dag")
        print("Variable dag_dependency_message has been set.")

    @task
    def ready_to_trigger():
        print("Triggering the downstream DAG now.")

    start = set_shared_variable()
    ready = ready_to_trigger()
    start >> ready


upstream_dag = upstream_dag()

from airflow.sdk.dag import dag
from airflow.sdk.task import task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


@dag(
    dag_id="downstream_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "dag_dependency"],
)
def downstream_dag():
    wait_for_trigger = ExternalTaskSensor(
        task_id="wait_for_upstream",
        external_dag_id="upstream_dag",
        external_task_id="ready_to_trigger",
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    @task
    def read_shared_variable():
        message = Variable.get("dag_dependency_message", default_var="No message found")
        print(f"Read from Airflow Variable: {message}")

    read_shared_variable_task = read_shared_variable()
    wait_for_trigger >> read_shared_variable_task


downstream_dag = downstream_dag()

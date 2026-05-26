from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


@dag(
    dag_id="dag_dependency_trigger_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "dag_dependency"],
)
def dag_dependency_trigger():
    @task
    def set_shared_variable():
        Variable.set("dag_dependency_message", "Hello from trigger dag")
        print("Variable dag_dependency_message has been set.")

    @task
    def ready_to_trigger():
        print("Triggering the downstream DAG now.")

    start = set_shared_variable()
    ready = ready_to_trigger()
    ready >> TriggerDagRunOperator(
        task_id="trigger_downstream_dag",
        trigger_dag_id="dag_dependency_target_dag",
        wait_for_completion=False,
    )


dag = dag_dependency_trigger()


@dag(
    dag_id="dag_dependency_target_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "dag_dependency"],
)
def dag_dependency_target():
    @task
    def read_shared_variable():
        message = Variable.get("dag_dependency_message", default_var="No message found")
        print(f"Read from Airflow Variable: {message}")

    read_shared_variable()


dag_target = dag_dependency_target()

from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="galaxy_pipeline_orchestrator",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["galaxy", "orchestration"],
)
def galaxy_pipeline_orchestrator():
    start = EmptyOperator(task_id="start")

    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="ingest_galaxy_data",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="ingest_galaxy_data",
        external_task_id="end",
        execution_date_fn=lambda dt: dt,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    trigger_quality_check = TriggerDagRunOperator(
        task_id="trigger_quality_check",
        trigger_dag_id="quality_check_galaxy_data",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    wait_for_quality_check = ExternalTaskSensor(
        task_id="wait_for_quality_check",
        external_dag_id="quality_check_galaxy_data",
        external_task_id="end",
        execution_date_fn=lambda dt: dt,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="transform_galaxy_data",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_transform",
        external_dag_id="transform_galaxy_data",
        external_task_id="end",
        execution_date_fn=lambda dt: dt,
        poke_interval=30,
        timeout=3600,
        mode="reschedule",
    )

    end = EmptyOperator(task_id="end")

    start >> trigger_ingestion >> wait_for_ingestion >> trigger_quality_check >> wait_for_quality_check >> trigger_transform >> wait_for_transform >> end


galaxy_pipeline_orchestrator()

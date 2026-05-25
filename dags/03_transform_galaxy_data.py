from __future__ import annotations

import os
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="transform_galaxy_data",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["galaxy", "databricks", "transform"],
)
def transform_galaxy_data():
    start = EmptyOperator(task_id="start")

    @task
    def prepare_transform_params() -> dict:
        """Build transform parameters from Airflow Variables."""
        return {
            "source_table": Variable.get("raw_galaxy_table", "raw_galaxies"),
            "target_table": Variable.get("curated_galaxy_table", "curated_galaxies"),
            "target_schema": Variable.get("target_schema", "space_demo"),
            "distance_threshold": Variable.get("galaxy_distance_threshold", "1000000"),
        }

    @task
    def confirm_transform(params: dict) -> str:
        """Confirm that the Databricks transform job was submitted."""
        print("Transform parameters:", params)
        return "transform_confirmed"

    transform_params = prepare_transform_params()

    submit_databricks_transform = DatabricksSubmitRunOperator(
        task_id="submit_databricks_transform",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "airflow_galaxy_transform_{{ ds_nodash }}",
            "existing_cluster_id": "{{ var.value.databricks_cluster_id }}",
            "notebook_task": {
                "notebook_path": "/Shared/airflow_demo/transform_galaxy_data",
                "base_parameters": {
                    "source_table": "{{ ti.xcom_pull(task_ids='prepare_transform_params')['source_table'] }}",
                    "target_table": "{{ ti.xcom_pull(task_ids='prepare_transform_params')['target_table'] }}",
                    "target_schema": "{{ ti.xcom_pull(task_ids='prepare_transform_params')['target_schema'] }}",
                    "run_date": "{{ ds }}",
                    "distance_threshold": "{{ ti.xcom_pull(task_ids='prepare_transform_params')['distance_threshold'] }}",
                    "partition_date": "{{ macros.galaxy_partition_date(ds) }}",
                },
            },
        },
    )

    end = EmptyOperator(task_id="end")

    start >> transform_params >> submit_databricks_transform >> confirm_transform(transform_params) >> end


transform_galaxy_data()

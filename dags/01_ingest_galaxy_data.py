from __future__ import annotations

import csv
import os
import sys
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from include.data.galaxy_api import get_galaxy_data

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="ingest_galaxy_data",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["galaxy", "databricks", "ingestion"],
)
def ingest_galaxy_data():
    start = EmptyOperator(task_id="start")

    @task
    def read_airflow_variables() -> dict:
        """Read Airflow Variables used by the ingestion DAG."""
        return {
            "galaxy_num_records": int(Variable.get("galaxy_num_records", default_var=10)),
            "galaxy_output_path": Variable.get("galaxy_output_path", "/opt/airflow/include/output"),
            "databricks_cluster_id": Variable.get("databricks_cluster_id", "your-databricks-cluster-id"),
            "target_schema": Variable.get("target_schema", "space_demo"),
            "raw_galaxy_table": Variable.get("raw_galaxy_table", "raw_galaxies"),
        }

    @task
    def extract_galaxy_data(num_records: int) -> list[dict]:
        """Mock an API call by loading galaxy data and returning a small list of rows."""
        galaxies = get_galaxy_data(num_records)
        return galaxies.to_dict(orient="records")

    @task
    def save_galaxy_file(rows: list[dict], output_path: str) -> dict:
        """Save galaxy data to a CSV file and expose metadata through XCom."""
        os.makedirs(output_path, exist_ok=True)
        output_file = os.path.join(
            output_path,
            f"galaxy_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv",
        )

        with open(output_file, "w", newline="", encoding="utf-8") as csv_file:
            if rows:
                writer = csv.DictWriter(csv_file, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)

        return {
            "output_file_path": output_file,
            "row_count": len(rows),
        }

    @task
    def prepare_ingestion_metadata(save_result: dict, variables: dict) -> dict:
        """Prepare metadata that will be passed into the Databricks notebook task."""
        metadata = {
            "output_file_path": save_result["output_file_path"],
            "row_count": save_result["row_count"],
            "ingestion_date": datetime.utcnow().isoformat(),
            "target_schema": variables["target_schema"],
            "target_table": variables["raw_galaxy_table"],
        }
        return metadata

    @task
    def confirm_ingestion(metadata: dict) -> str:
        """Confirm that the ingestion metadata is available after the Databricks submission."""
        print("Ingestion metadata:", metadata)
        return "ingestion_confirmed"

    read_vars = read_airflow_variables()

    with TaskGroup("extract_group"):
        galaxy_rows = extract_galaxy_data(read_vars["galaxy_num_records"])
        save_result = save_galaxy_file(galaxy_rows, read_vars["galaxy_output_path"])

    metadata = prepare_ingestion_metadata(save_result, read_vars)

    with TaskGroup("databricks_ingestion_group"):
        submit_databricks_ingestion = DatabricksSubmitRunOperator(
            task_id="submit_databricks_ingestion",
            databricks_conn_id="databricks_default",
            json={
                "run_name": "airflow_galaxy_ingestion_{{ ds_nodash }}",
                "existing_cluster_id": "{{ var.value.databricks_cluster_id }}",
                "notebook_task": {
                    "notebook_path": "/Shared/airflow_demo/ingest_galaxy_data",
                    "base_parameters": {
                        "input_file_path": "{{ ti.xcom_pull(task_ids='prepare_ingestion_metadata')['output_file_path'] }}",
                        "target_schema": "{{ ti.xcom_pull(task_ids='prepare_ingestion_metadata')['target_schema'] }}",
                        "target_table": "{{ ti.xcom_pull(task_ids='prepare_ingestion_metadata')['target_table'] }}",
                        "run_date": "{{ ds }}",
                        "row_count": "{{ ti.xcom_pull(task_ids='prepare_ingestion_metadata')['row_count'] }}",
                    },
                },
            },
        )

        confirm_databricks = confirm_ingestion(metadata)
        submit_databricks_ingestion >> confirm_databricks

    end = EmptyOperator(task_id="end")

    start >> read_vars >> galaxy_rows >> save_result >> metadata >> submit_databricks_ingestion >> confirm_databricks >> end


ingest_galaxy_data()

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from plugins.custom_operators.galaxy_validation_operator import GalaxyValidationOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
}


@dag(
    dag_id="quality_check_galaxy_data",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["galaxy", "quality", "validation"],
)
def quality_check_galaxy_data():
    start = EmptyOperator(task_id="start")

    @task
    def load_quality_config() -> dict:
        """Load quality configuration from Airflow Variables."""
        required_columns = Variable.get(
            "galaxy_required_columns",
            default_var="name,distance_from_milkyway,distance_from_solarsystem,type_of_galaxy,characteristics",
        )
        return {
            "galaxy_quality_min_rows": int(Variable.get("galaxy_quality_min_rows", default_var=5)),
            "galaxy_required_columns": [col.strip() for col in required_columns.split(",") if col.strip()],
            "target_table": Variable.get("raw_galaxy_table", "raw_galaxies"),
        }

    @task
    def publish_quality_result(results: list[dict]) -> str:
        """Publish a summary of all quality checks."""
        print("Galaxy quality results:")
        for result in results:
            print(result)
        return "quality_published"

    config = load_quality_config()

    raw_table = Variable.get("raw_galaxy_table", "raw_galaxies")
    min_rows = int(Variable.get("galaxy_quality_min_rows", default_var=5))
    required_columns = [
        col.strip()
        for col in Variable.get(
            "galaxy_required_columns",
            default_var="name,distance_from_milkyway,distance_from_solarsystem,type_of_galaxy,characteristics",
        ).split(",")
        if col.strip()
    ]

    with TaskGroup("quality_checks_group"):
        validate_schema = GalaxyValidationOperator(
            task_id="validate_schema",
            table_name=raw_table,
            min_rows=min_rows,
            required_columns=required_columns,
        )

        validate_row_count = GalaxyValidationOperator(
            task_id="validate_row_count",
            table_name=raw_table,
            min_rows=min_rows,
            required_columns=required_columns,
        )

        validate_required_columns = GalaxyValidationOperator(
            task_id="validate_required_columns",
            table_name=raw_table,
            min_rows=min_rows,
            required_columns=required_columns,
        )

    end = EmptyOperator(task_id="end")

    start >> config >> [validate_schema, validate_row_count, validate_required_columns] >> publish_quality_result([validate_schema, validate_row_count, validate_required_columns]) >> end


quality_check_galaxy_data()

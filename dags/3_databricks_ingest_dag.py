from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime


@dag(
    dag_id="3_databricks_ingest_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "databricks"],
)
def databricks_ingest():
    table_name = Variable.get("databricks_ingest_table", default_var="demo_ingest_table")

    DatabricksSubmitRunOperator(
        task_id="submit_databricks_ingest_job",
        databricks_conn_id="databricks_default",
        json={
            "new_cluster": {
                "spark_version": "13.1.x-scala2.12",
                "node_type_id": "Standard_D3_v2",
                "num_workers": 1,
            },
            "spark_python_task": {
                "python_file": "dbfs:/FileStore/scripts/ingest_to_table.py",
                "parameters": ["--table", table_name],
            },
        },
    )


dag = databricks_ingest()

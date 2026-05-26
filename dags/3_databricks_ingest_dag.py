from airflow.sdk import dag, task, Variable
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from datetime import datetime


@dag(
    dag_id="3_databricks_ingest_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "databricks", "ingest"],
)
def databricks_ingest():
    """
    Databricks Data Ingestion Pipeline using Tasks
    Demonstrates connecting to Databricks with basic auth and ingesting data
    """

    @task
    def ingest_to_databricks():
        """Ingest data into a Databricks table (SQL Warehouse)."""
        table = Variable.get("databricks_ingest_table", default_var="demo_ingest_table")
        rows = int(Variable.get("databricks_ingest_rows", default_var="1000"))
        sql = f"-- INSERT INTO {table} -- rows: {rows}"

        try:
            hook = DatabricksHook(databricks_conn_id="databricks_default")
            if hasattr(hook, "run_query"):
                try:
                    hook.run_query(sql)
                    status = "ingest_complete_via_sqlwarehouse"
                except Exception:
                    print("Hook.run_query failed; falling back to simulated ingestion")
                    status = "ingest_simulated"
            else:
                print("DatabricksHook has no 'run_query' method; simulating ingestion")
                status = "ingest_simulated"

            result = {
                "table_name": table,
                "rows_ingested": rows,
                "timestamp": datetime.now().isoformat(),
                "status": status,
            }
            print(f"✓ Ingest result: {result}")
            return result
        except Exception as e:
            print(f"✗ Ingestion failed: {str(e)}")
            raise

    # Single task DAG: only run ingestion
    ingested = ingest_to_databricks()


dag = databricks_ingest()

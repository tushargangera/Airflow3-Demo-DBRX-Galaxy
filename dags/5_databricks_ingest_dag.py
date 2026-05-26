from airflow.sdk import dag, task, Variable
from airflow.hooks.base import BaseHook
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from datetime import datetime
from include.data.galaxy_api import get_galaxy_data


REQUIRED_CONNECTIONS = ["databricks_default"]


def _sql_quote(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


@dag(
    dag_id="5_databricks_ingest_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "databricks", "ingest"],
)
def databricks_ingest():
    """
    Databricks ingest pipeline for JSON galaxy data.

    Task 1 validates the required Databricks Airflow connection.
    Task 2 loads galaxy JSON data into a Databricks SQL Warehouse table.
    """

    @task
    def validate_connections():
        missing = []

        for connection_id in REQUIRED_CONNECTIONS:
            try:
                conn = BaseHook.get_connection(connection_id)
                print(
                    f"Validated connection '{connection_id}': host={conn.host}, login={conn.login}, extras={conn.extra_dejson}"
                )
            except Exception as exc:
                missing.append(f"{connection_id}: {exc}")

        if missing:
            raise ValueError(f"Missing required Airflow connections: {', '.join(missing)}")

        return "connections_validated"

    @task
    def ingest_galaxy_data():
        table = Variable.get("databricks_galaxy_table", default="galaxy_data")
        rows_to_ingest = int(Variable.get("databricks_galaxy_rows", default="10"))

        galaxy_df = get_galaxy_data(rows_to_ingest)
        galaxy_records = galaxy_df.to_dict(orient="records")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            name STRING,
            distance_from_milkyway INT,
            distance_from_solarsystem INT,
            type_of_galaxy STRING,
            characteristics STRING
        )
        USING DELTA
        """

        values_sql = ",\n".join(
            "(" + ", ".join(
                [
                    _sql_quote(record["name"]),
                    _sql_quote(record["distance_from_milkyway"]),
                    _sql_quote(record["distance_from_solarsystem"]),
                    _sql_quote(record["type_of_galaxy"]),
                    _sql_quote(record["characteristics"]),
                ]
            ) + ")"
            for record in galaxy_records
        )

        insert_sql = f"INSERT INTO {table} VALUES\n{values_sql}"

        hook = DatabricksHook(databricks_conn_id="databricks_default")
        hook.run_query(create_table_sql)
        hook.run_query(insert_sql)

        result = {
            "table": table,
            "ingested_rows": len(galaxy_records),
            "generated_at": datetime.now().isoformat(),
        }
        print(f"✓ Ingested galaxy JSON data into Databricks table: {result}")
        return result

    validated = validate_connections()
    ingested = ingest_galaxy_data()
    validated >> ingested


dag = databricks_ingest()

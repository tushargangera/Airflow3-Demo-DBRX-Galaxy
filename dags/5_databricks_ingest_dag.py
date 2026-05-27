from airflow.sdk import dag, task, Variable, BaseHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from datetime import datetime
from include.data.galaxy_api import get_galaxy_data
from plugins.galaxy_sql_utils import sql_quote


REQUIRED_CONNECTIONS = ["databricks_default"]


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
            raise ValueError(
                f"Missing required Airflow connections: {', '.join(missing)}"
            )

        return "connections_validated"

    @task
    def ingest_galaxy_data():
        table = Variable.get("databricks_galaxy_table")
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
            "("
            + ", ".join(
                [
                    sql_quote(record["name"]),
                    sql_quote(record["distance_from_milkyway"]),
                    sql_quote(record["distance_from_solarsystem"]),
                    sql_quote(record["type_of_galaxy"]),
                    sql_quote(record["characteristics"]),
                ]
            )
            + ")"
            for record in galaxy_records
        )

        # http_path (e.g. "/sql/1.0/warehouses/<warehouse-id>") or sql_endpoint_name
        # must be supplied so the hook targets a SQL Warehouse. Pull from Airflow
        # Variables so the warehouse can be swapped per environment without code changes.
        http_path = Variable.get("databricks_http_path", default=None)
        sql_endpoint_name = Variable.get("databricks_sql_endpoint_name", default=None)

        hook = DatabricksSqlHook(
            databricks_conn_id="databricks_default",
            http_path=http_path,
            sql_endpoint_name=sql_endpoint_name,
        )
        hook.run(create_table_sql)

        if galaxy_records:
            insert_sql = f"INSERT INTO {table} VALUES\n{values_sql}"
            hook.run(insert_sql)
        else:
            print(f"No galaxy records to ingest into {table}.")

        result = {
            "table": table,
            "ingested_rows": len(galaxy_records),
            "generated_at": datetime.now().isoformat(),
        }
        print(f"Ingested galaxy JSON data into Databricks table: {result}")
        return result

    validated = validate_connections()
    ingested = ingest_galaxy_data()
    validated >> ingested


dag = databricks_ingest()

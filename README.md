# Airflow 3 DBRX Galaxy Demo

This repository is an Astro/Airflow 3 demo project that walks through basic DAG
patterns and a Databricks SQL ingest workflow. The Databricks example generates
mock galaxy data with pandas and writes it into a Delta table through the
Databricks Airflow provider.

## What is included

- `dags/1_hello_world_dag.py` - minimal TaskFlow DAG that returns a greeting.
- `dags/2_xcom_example_dag.py` - pushes and pulls values through XCom.
- `dags/3_etl_upstream_dag.py` - fan-out/fan-in ETL example that triggers another DAG.
- `dags/4_downstream_dag.py` - downstream DAG used by the upstream trigger example.
- `dags/5_databricks_ingest_dag.py` - validates Databricks connectivity and ingests galaxy data.
- `include/custom_functions/galaxy_functions.py` - mock galaxy API/data generator.
- `include/data/galaxy_api.py` - compatibility wrapper used by the Databricks DAG.
- `config/variables.json` - sample Airflow variables.
- `config/connections.json` - sample Airflow connections placeholder.

## Prerequisites

- Astro CLI
- Docker Desktop or another Docker runtime supported by Astro
- Databricks workspace access for the Databricks ingest DAG
- A Databricks SQL Warehouse for table writes

The project uses the Astro Runtime image in `Dockerfile` and installs the
project-specific Python packages from `requirements.txt`.

## Quick start

Start Airflow locally:

```bash
astro dev start
```

Open the Airflow UI, then trigger any of the example DAGs manually. All DAGs are
scheduled with `@daily`, use a `2025-01-01` start date, and have `catchup=False`
so they are easy to run as demos.

To validate that the DAG files parse in Astro:

```bash
astro dev parse
```

## Databricks setup

The Databricks ingest DAG expects an Airflow connection named
`databricks_default`.

Create that connection in Airflow with your Databricks workspace details and
token. Do not commit personal access tokens to this repository.

The DAG also reads these Airflow variables:

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `databricks_galaxy_table` | No | `galaxy_data` | Target Delta table name. Use a fully qualified name such as `catalog.schema.galaxy_data` when needed. |
| `databricks_galaxy_rows` | No | `10` | Number of mock galaxy rows to ingest. Keep this between `1` and `20`. |
| `databricks_http_path` | Use this or `databricks_sql_endpoint_name` | `None` | SQL Warehouse HTTP path, for example `/sql/1.0/warehouses/<warehouse-id>`. |
| `databricks_sql_endpoint_name` | Use this or `databricks_http_path` | `None` | SQL Warehouse endpoint name. |

Example variable payload:

```json
{
  "databricks_galaxy_table": "main.bronze.galaxy_data",
  "databricks_galaxy_rows": "10",
  "databricks_http_path": "/sql/1.0/warehouses/<warehouse-id>"
}
```

Import variables and connections through the Airflow UI, or use the Airflow CLI
inside the running Astro environment.

## Demo flow

1. Run `1_hello_world_dag` to confirm the local Airflow environment is working.
2. Run `2_xcom_example_dag` to inspect task return values and explicit XCom keys.
3. Run `3_etl_upstream_dag` to see parallel extraction, consolidation,
   transformation, loading, and a DAG-to-DAG trigger.
4. Check `4_downstream_dag` after the upstream DAG runs.
5. Configure Databricks and run `5_databricks_ingest_dag` to create and populate
   the galaxy Delta table.

## Notes

- `config/connections.json` is intentionally empty so secrets are not stored in
  source control.
- The mock galaxy data source contains 20 records.
- The Databricks DAG creates the target table if it does not already exist and
  inserts the generated galaxy rows.

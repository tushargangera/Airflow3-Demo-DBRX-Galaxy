# Airflow 3 DBRX Galaxy Demo

Short Airflow 3 demo project for explaining core Airflow concepts with a small
Databricks SQL ingest example.

## What This Demo Covers

- DAGs and TaskFlow API
- Schedules, retries, and email alerts
- XCom task communication
- Airflow Variables
- DAG-to-DAG triggering
- Backfill
- Airflow Connections
- Databricks SQL ingest into a Delta table

## Project Files

- `dags/1_hello_world_dag.py` - basic TaskFlow DAG. Runs daily at 5 AM.
- `dags/2_xcom_example_dag.py` - XCom push, pull, and task return example. Runs weekdays at 8 AM.
- `dags/3_etl_upstream_dag.py` - fan-out/fan-in ETL flow and downstream DAG trigger.
- `dags/4_downstream_dag.py` - downstream DAG that writes an Airflow Variable.
- `dags/5_databricks_ingest_dag.py` - validates Databricks connection and loads galaxy data.
- `include/custom_functions/galaxy_functions.py` - mock galaxy data generator.
- `include/data/galaxy_api.py` - wrapper used by the Databricks DAG.
- `plugins/galaxy_sql_utils.py` - reusable SQL helper used by DAG 5.
- `config/variables.json` - sample Airflow Variables.
- `config/connections.json` - empty placeholder. Do not store secrets here.
- `DEMO_FLOW.md` - presenter flow.
- `DEMO_TRANSCRIPT.md` - read-aloud demo script.

## Requirements

- A running Airflow 3 environment
- Databricks workspace access for DAG 5
- Databricks SQL Warehouse for DAG 5

## Run Locally

Start your Airflow 3 environment with your preferred local setup.

Validate DAG parsing:

```bash
airflow dags list
```

Import variables if you keep them in `config/variables.json`:

```bash
airflow variables import config/variables.json
```

Open the Airflow UI and trigger the DAGs manually for the demo.

Before running DAG 5, confirm the Databricks variables listed below exist.

## DAG Settings

- All DAGs use `start_date=datetime(2025, 1, 1)`.
- All DAGs use `catchup=False` for easier demos.
- DAG 1 runs at `0 5 * * *`.
- DAG 2 runs at `0 8 * * 1-5`.
- DAG 1 and DAG 2 retry failed tasks twice with a 5-minute delay.
- DAG 1 and DAG 2 email on failure and do not email on retry.

Update the email addresses in the DAG files before using email alerts in a real
environment.

## Databricks Setup

DAG 5 expects an Airflow connection named:

```text
databricks_default
```

Create this connection in the Airflow UI with your Databricks workspace host and
token. Do not commit tokens to this repository.

DAG 5 reads these Airflow Variables:

| Variable | Required | Purpose |
| --- | --- | --- |
| `databricks_galaxy_table` | Yes | Target Delta table, for example `catalog.schema.galaxy_data`. |
| `databricks_galaxy_rows` | No | Number of mock rows to ingest. Default is `10`. |
| `databricks_http_path` | Use this or endpoint name | SQL Warehouse HTTP path. |
| `databricks_sql_endpoint_name` | Use this or HTTP path | SQL Warehouse endpoint name. |

Example variables:

```json
{
  "databricks_galaxy_table": "main.bronze.galaxy_data",
  "databricks_galaxy_rows": "10",
  "databricks_http_path": "/sql/1.0/warehouses/<warehouse-id>"
}
```

## Demo Order

1. Run `1_hello_world_dag` to show a simple DAG and task log.
2. Run `2_xcom_example_dag` to show XCom and task return values.
3. Run `3_etl_upstream_dag` to show fan-out, fan-in, and DAG triggering.
4. Open `4_downstream_dag` to show the triggered downstream run.
5. Show Variables and Connections in the Airflow UI.
6. Backfill `1_hello_world_dag` for a safe historical-run demo.
7. Run `5_databricks_ingest_dag` after Databricks is configured.

Backfill example:

```bash
airflow backfill create --dag-id 1_hello_world_dag --from-date 2025-01-01 --to-date 2025-01-03 --reprocess-behavior none --max-active-runs 2
```

## Notes

- Use DAG 1 for backfill demos because it has no external side effects.
- Do not backfill DAG 5 unless duplicate inserts are acceptable.
- XCom is for small values only, not large datasets.
- Keep credentials in Airflow Connections or a secrets backend.

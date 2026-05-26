# Airflow 3 Demo

Simple Airflow 3 examples with four DAGs:

- `hello_world_dag.py`: hello world task
- `xcom_example_dag.py`: XCom example
- `databricks_ingest_dag.py`: Databricks run using an Airflow Variable
- `dag_dependency_example_dag.py`: DAG-to-DAG trigger example

## Quick start

1. Load `config/variables.json` into Airflow Variables.
2. Load `config/connections.json` into Airflow Connections.
3. Ensure `databricks_default` exists.
4. Trigger a DAG from the Airflow UI.

## Keys

- `databricks_ingest_table` controls the target table name.
- All DAGs use `@daily` scheduling.
- `databricks_ingest_dag.py` reads its table name from an Airflow Variable.
- `dag_dependency_example_dag.py` triggers a second DAG.

## Files

- `dags/` — example DAGs
- `config/variables.json` — variable definitions
- `config/connections.json` — connection definitions

## Notes

- This repo is intentionally minimal.
- No custom plugins are required.

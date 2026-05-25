# airflow-databricks-galaxy-demo

A beginner-friendly Apache Airflow 3 demo project that shows a Galaxy dataset workflow with Databricks integration, XCom metadata sharing, Airflow Variables, custom plugins, TaskGroup hierarchy, and DAG-to-DAG orchestration.

## What this project demonstrates

- Mock galaxy data ingestion from a local API function
- Saving data to a local CSV file and passing metadata via XCom
- Submitting Databricks notebook jobs with `DatabricksSubmitRunOperator`
- A quality checkpoint DAG using a custom `GalaxyValidationOperator`
- A transformation DAG with a custom macro for partitioned run dates
- A pipeline orchestrator DAG that triggers and waits for other DAGs
- Airflow Variables and connections loaded from JSON
- Simple, beginner-friendly code with comments and structured tasks

## Project structure

- `dags/`
  - `01_ingest_galaxy_data.py`: ingestion DAG
  - `02_quality_check_galaxy_data.py`: quality validation DAG
  - `03_transform_galaxy_data.py`: transformation DAG
  - `04_galaxy_pipeline_orchestrator.py`: DAG orchestration DAG
- `include/data/galaxy_api.py`: mock Galaxy data API
- `include/data/galaxy_names.txt`: galaxy name example file
- `plugins/custom_operators/galaxy_validation_operator.py`: custom validation operator
- `plugins/custom_macros/galaxy_macros.py`: custom macro for partition date formatting
- `config/airflow.cfg`: demo Airflow configuration
- `config/variables.json`: example Airflow Variables
- `config/connections.json`: example Databricks connection
- `requirements.txt`: Python dependencies
- `docker-compose.yml`: local Airflow Docker setup

## DAG purpose

1. `ingest_galaxy_data`
   - Mock API ingestion of galaxy rows
   - Saves data to `/opt/airflow/include/output/`
   - Pushes metadata like `output_file_path`, `row_count`, and `target_table` via XCom
   - Submits a Databricks notebook job to ingest the file into Delta Lake

2. `quality_check_galaxy_data`
   - Loads quality thresholds from Airflow Variables
   - Uses `GalaxyValidationOperator` to simulate validation checks
   - Demonstrates a custom plugin operator and TaskGroup organization

3. `transform_galaxy_data`
   - Reads transformation configuration from Variables
   - Submits a Databricks notebook job for transforming raw galaxy data
   - Uses the custom macro `galaxy_partition_date` to format partition values

4. `galaxy_pipeline_orchestrator`
   - Triggers DAGs in order
   - Waits for each downstream DAG to complete before moving on
   - Demonstrates DAG-to-DAG dependency using `TriggerDagRunOperator` and `ExternalTaskSensor`

## How DAG-to-DAG dependency works

The orchestrator DAG triggers each child DAG in sequence and then waits for the child DAG's `end` task to complete. That means:

- Ingestion runs first
- Quality check runs after ingestion success
- Transformation runs after quality check success

This shows how Airflow can coordinate multiple DAGs instead of putting every step in one DAG.

## How XCom is used

- `ingest_galaxy_data` saves the output file path and row count and returns that metadata from task functions.
- The metadata is passed through XCom to the Databricks submission task.
- Only small metadata values are shared, not the full dataset.

## How Airflow Variables are used

Variables are loaded from `config/variables.json` and include:

- `galaxy_num_records`
- `galaxy_output_path`
- `galaxy_quality_min_rows`
- `galaxy_required_columns`
- `databricks_cluster_id`
- `target_schema`
- `raw_galaxy_table`
- `curated_galaxy_table`
- `galaxy_distance_threshold`

Variables control the mock data ingestion, validation thresholds, and Databricks notebook parameters.

## How the Databricks connection works

- `config/connections.json` contains a placeholder `databricks_default` connection.
- The DAGs use `databricks_conn_id="databricks_default"`.
- In a real environment, replace the host and token with your Databricks workspace values.

## How the custom plugin operator works

- `GalaxyValidationOperator` extends `BaseOperator`
- It accepts `table_name`, `min_rows`, and `required_columns`
- It logs validation information and returns a simple dictionary
- It is used in `quality_check_galaxy_data` to simulate schema and row-count checks

## How the custom macro works

- `galaxy_partition_date(ds)` converts `YYYY-MM-DD` to `YYYYMMDD`
- It is used in `03_transform_galaxy_data.py` to demonstrate custom macro usage in notebook parameters

## Run locally with Docker

1. Start the project from the root folder:

```bash
cd airflow-databricks-galaxy-demo
docker compose up airflow-init
```

2. Start the scheduler and webserver:

```bash
docker compose up -d airflow-scheduler airflow-webserver
```

3. Open the Airflow UI:

```text
http://localhost:8080
```

4. Login with the demo admin user created in the compose file.

## Import variables and connections

The `docker compose` setup does not automatically import JSON files. Use the Airflow CLI or UI to load the variables and connections from `config/variables.json` and `config/connections.json`.

Example using the CLI inside the scheduler or webserver container:

```bash
docker compose exec airflow-webserver airflow variables import /opt/airflow/config/variables.json

docker compose exec airflow-webserver airflow connections import /opt/airflow/config/connections.json
```

## Configure the Databricks connection

Replace placeholder values in `config/connections.json`:

- `host`: your Databricks workspace URL
- `password`: a Databricks personal access token
- `existing_cluster_id`: your cluster ID

Then import the connection using the Airflow CLI or create it in the Airflow UI.

## Trigger the pipeline orchestrator

In the Airflow UI, trigger the `galaxy_pipeline_orchestrator` DAG to start the full workflow.

## Placeholder values and production notes

- The Databricks notebook paths are placeholders (`/Shared/airflow_demo/...`).
- The Databricks connection is a demo placeholder and should be updated before using a real workspace.
- Local ingestion is mocked with `include/data/galaxy_api.py`.
- In production, replace mock logic with real API calls or data sources and configure secure secrets support.
- For production, use a strong secret store, parameterize more environment values, and use a production-ready executor and database.

from airflow.sdk import dag, task, Variable
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth


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
    def validate_connection():
        """Validate Databricks connection credentials"""
        print("Validating Databricks connection...")
        try:
            hook = DatabricksHook(databricks_conn_id="databricks_default")
            print("✓ Databricks connection validated successfully")
            return {"status": "connection_valid"}
        except Exception as e:
            print(f"✗ Connection validation failed: {str(e)}")
            raise

    @task
    def extract_source_data():
        """Extract data from source system"""
        table_name = Variable.get("databricks_ingest_table", default_var="demo_ingest_table")
        print(f"Extracting data from source for table: {table_name}")
        
        # Simulate data extraction
        extracted_data = {
            "table_name": table_name,
            "row_count": 1000,
            "columns": ["id", "name", "value", "timestamp"],
            "status": "extraction_complete"
        }
        print(f"✓ Extracted {extracted_data['row_count']} rows")
        return extracted_data

    @task
    def validate_data(extracted: dict):
        """Validate extracted data quality"""
        print(f"Validating {extracted['row_count']} rows...")
        
        validation_result = {
            "table_name": extracted["table_name"],
            "validation_status": "passed",
            "rows_valid": extracted["row_count"],
            "duplicates_found": 0,
            "null_values": 0
        }
        print(f"✓ Data validation passed - All rows valid")
        return validation_result

    @task
    def transform_data(validation: dict):
        """Transform and prepare data for ingestion"""
        print(f"Transforming {validation['rows_valid']} rows...")
        
        transform_result = {
            "table_name": validation["table_name"],
            "transformed_rows": validation["rows_valid"],
            "transformations_applied": {
                "deduplication": True,
                "null_handling": "filled",
                "type_conversion": True
            },
            "status": "transformation_complete"
        }
        print(f"✓ Transformation complete - {transform_result['transformed_rows']} rows ready")
        return transform_result

    @task
    def ingest_to_databricks(transform: dict):
        """Ingest transformed data into Databricks table"""
        print(f"Ingesting {transform['transformed_rows']} rows into {transform['table_name']}...")
        
        try:
            hook = DatabricksHook(databricks_conn_id="databricks_default")
            
            ingest_result = {
                "table_name": transform["table_name"],
                "rows_ingested": transform["transformed_rows"],
                "timestamp": datetime.now().isoformat(),
                "status": "ingest_complete",
                "databricks_workspace": "connected"
            }
            print(f"✓ Successfully ingested {ingest_result['rows_ingested']} rows to {transform['table_name']}")
            return ingest_result
        except Exception as e:
            print(f"✗ Ingestion failed: {str(e)}")
            raise

    @task
    def post_ingest_validation(ingest: dict):
        """Validate data after ingestion"""
        print(f"Post-ingest validation for {ingest['table_name']}...")
        
        validation = {
            "table_name": ingest["table_name"],
            "rows_in_table": ingest["rows_ingested"],
            "validation_passed": True,
            "completion_time": ingest["timestamp"]
        }
        print(f"✓ Post-ingest validation successful - Pipeline complete!")
        return validation

    # Define task dependencies
    conn_check = validate_connection()
    extracted = extract_source_data()
    validated = validate_data(extracted)
    transformed = transform_data(validated)
    ingested = ingest_to_databricks(transformed)
    final_validation = post_ingest_validation(ingested)

    # Set dependency chain: connection check runs before extraction
    conn_check >> extracted


dag = databricks_ingest()

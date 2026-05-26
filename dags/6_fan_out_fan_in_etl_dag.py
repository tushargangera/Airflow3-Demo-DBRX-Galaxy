from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="6_fan_out_fan_in_etl_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "etl", "fan_out_fan_in"],
)
def fan_out_fan_in_etl():
    """
    ETL Pipeline with Fan-Out/Fan-In Pattern
    
    Demonstrates a common workflow pattern where multiple tasks run in parallel
    (fan-out), then consolidate results (fan-in) before final processing.
    
    Task dependency tree:
    start_pipeline >> [extract_mysql, extract_postgres, extract_api] >> consolidate_data >> transform_data >> load_warehouse
    """

    @task
    def start_pipeline():
        """Initialize the ETL pipeline"""
        print("Starting ETL pipeline execution")

    @task
    def extract_mysql():
        """Extract data from MySQL database"""
        print("Extracting data from MySQL source")
        return {"source": "mysql", "records": 5000}

    @task
    def extract_postgres():
        """Extract data from PostgreSQL database"""
        print("Extracting data from PostgreSQL source")
        return {"source": "postgres", "records": 3500}

    @task
    def extract_api():
        """Extract data from REST API"""
        print("Extracting data from REST API endpoint")
        return {"source": "api", "records": 2200}

    @task
    def consolidate_data(mysql_data: dict, postgres_data: dict, api_data: dict):
        """Consolidate data from all sources"""
        total_records = mysql_data["records"] + postgres_data["records"] + api_data["records"]
        print(f"Consolidating data from 3 sources. Total records: {total_records}")
        return {"consolidated": True, "total_records": total_records}

    @task
    def transform_data(consolidated: dict):
        """Transform and clean the consolidated data"""
        print(f"Transforming {consolidated['total_records']} records")
        return {"transformed": True, "records": consolidated["total_records"]}

    @task
    def load_warehouse(transformed: dict):
        """Load data into data warehouse"""
        print(f"Loading {transformed['records']} transformed records to warehouse")
        return "ETL pipeline completed successfully"

    # Define the fan-out/fan-in dependency tree
    start = start_pipeline()
    
    # Fan-out: Three parallel extraction tasks
    mysql_data = extract_mysql()
    postgres_data = extract_postgres()
    api_data = extract_api()
    
    # Fan-in: Consolidate all data
    consolidated = consolidate_data(mysql_data, postgres_data, api_data)
    
    # Continue pipeline: Transform and Load
    transformed = transform_data(consolidated)
    load_warehouse(transformed)
    
    # Set start dependency
    start >> [mysql_data, postgres_data, api_data]


fan_out_fan_in_etl = fan_out_fan_in_etl()

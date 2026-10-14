from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagBag, DagModel
from datetime import datetime, timedelta
import json
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'migrate_dag_metadata',
    default_args=default_args,
    description='A simple DAG to migrate metadata',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

SOURCE_DAG_FOLDER = os.getenv('SOURCE_DAG_FOLDER', '/dags')
TARGET_DAG_FOLDER = os.getenv('TARGET_DAG_FOLDER', '/dags')
METADATA_FILE_PATH = os.getenv('METADATA_FILE_PATH', '/include/dags_metadata.json')
SOURCE_DB_URL = os.getenv('SOURCE_DB_URL', 'postgresql+psycopg2://postgres:postgres@source_db:5432/airflow')
TARGET_DB_URL = os.getenv('TARGET_DB_URL', 'postgresql+psycopg2://postgres:postgres@target_db:5432/airflow')

def create_session(db_url):
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    return Session()

def extract_dag_metadata(**kwargs):
    try:
        source_dagbag = DagBag(dag_folder=SOURCE_DAG_FOLDER)
        dags_metadata = {}
        for dag_id in source_dagbag.dags:
            dag_obj = source_dagbag.get_dag(dag_id)
            dags_metadata[dag_id] = {
                'dag_id': dag_obj.dag_id,
                'schedule_interval': str(dag_obj.schedule_interval),
                'default_args': dag_obj.default_args,
                'tags': dag_obj.tags,
            }
        with open(METADATA_FILE_PATH, 'w') as f:
            json.dump(dags_metadata, f)
        logging.info(f"DAG metadata extracted successfully to {METADATA_FILE_PATH}")
    except Exception as e:
        logging.error(f"Error extracting DAG metadata: {e}")
        raise

def load_dag_metadata(**kwargs):
    try:
        with open(METADATA_FILE_PATH, 'r') as f:
            dags_metadata = json.load(f)
        session = create_session(TARGET_DB_URL)
        for dag_id, metadata in dags_metadata.items():
            dag_model = DagModel(
                dag_id=metadata['dag_id'],
                is_paused=False,
                is_subdag=False,
                fileloc=TARGET_DAG_FOLDER,
                owners=metadata['default_args']['owner'],
                description='Migrated DAG',
            )
            dag_model.schedule_interval = metadata['schedule_interval']
            dag_model.default_view = 'tree'
            dag_model.tags = metadata['tags']
            session.add(dag_model)
        session.commit()
        logging.info("DAG metadata loaded successfully into target database")
    except Exception as e:
        logging.error(f"Error loading DAG metadata: {e}")
        session.rollback()
        raise
    finally:
        session.close()

extract_task = PythonOperator(
    task_id='extract_dag_metadata',
    python_callable=extract_dag_metadata,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_dag_metadata',
    python_callable=load_dag_metadata,
    provide_context=True,
    dag=dag,
)

extract_task >> load_task

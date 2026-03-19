"""
Apache Airflow DAG for orchestrating the Global Factory Intelligence (GFI) pipeline.
Executes the Ingestion, Extraction, and Loading stages sequentially.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments applied to all tasks
default_args = {
    'owner': 'hazem_elgendy',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    dag_id='gfi_daily_etl',
    default_args=default_args,
    description='Daily orchestration of the GFI synthetic data pipeline',
    schedule='0 0 * * *',  # Runs at midnight every day
    # A past date ensures it's ready to trigger immediately
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Prevents Airflow from running backfills for every day since the start_date
    tags=['gfi', 'maintenance', 'etl'],
) as dag:

    # Task 1: Generate Raw Logs (Sprint 1)
    # Using the standard Python image command inside the container
    generate_raw_logs = BashOperator(
        task_id='generate_raw_logs',
        bash_command='python include/Ingestion/gfi_001_ingestion.py',
        cwd='/usr/local/airflow',  # Explicitly set the working directory
    )

    # Task 2: Extract & Structure Logs (Sprint 2)
    # Update the script name here if you named the async version differently
    extract_and_structure_logs = BashOperator(
        task_id='extract_and_structure_logs',
        bash_command='python include/Extraction/gfi_002_extraction.py',
        cwd='/usr/local/airflow',
    )

    # Task 3: Load to BigQuery (Sprint 3)
    load_to_bigquery = BashOperator(
        task_id='load_to_bigquery',
        bash_command='python include/Loading/gfi_003_load_bigquery.py',
        cwd='/usr/local/airflow',
    )

    # AC4: Define Task Dependencies (The Pipeline Flow)
    generate_raw_logs >> extract_and_structure_logs >> load_to_bigquery

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import storage
import logging
import csv
import os

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Define the GCS bucket and file
GCS_BUCKET = 'ntt_cycling_bucket'
GCS_FILE_PATH = '/opt/airflow/data/raw/data.csv'

# Define schema
SCHEMA = {
    "Year": str,
    "UnqID": str,
    "Date": str,
    "Weather": str,
    "Time": str,
    "Day": str,
    "Round": str,
    "Dir": str,
    "Path": str,
    "Mode": str,
    "Count": str,
}

# Function to validate GCS data against schema
def validate_gcs_data(bucket_name, file_path, schema):
    """
    Validate data stored in GCS before loading it into BigQuery.
    Raise an exception if validation fails.
    """
    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)

        # Download the file from GCS
        local_path = f'/tmp/{os.path.basename(file_path)}'
        blob.download_to_filename(local_path)
        logging.info(f"Downloaded {file_path} from GCS to {local_path}")

        # Open and validate CSV file
        with open(local_path, mode='r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Validate each field against the schema
                for field, field_type in schema.items():
                    if field not in row or row[field] == "":
                        logging.error(f"Validation failed: {field} is missing or null in row {row}")
                        raise ValueError(f"Data validation failed: {field} is missing or null.")
                    if not isinstance(row[field], field_type):
                        logging.error(f"Validation failed: {field} in row {row} is not of type {field_type}")
                        raise ValueError(f"Data validation failed: {field} is not of type {field_type}.")

        logging.info("Data validation passed: All rows meet the schema criteria.")

    except Exception as e:
        logging.error(f"Error during data validation: {str(e)}")
        raise

# DAG definition
dag = DAG(
    dag_id="gcs_to_bigquery_dag",
    default_args=default_args,
    description="A DAG to move data from GCS to BigQuery with validation",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
)

# Data validation task
validate_data_task = PythonOperator(
    task_id="validate_gcs_data",
    python_callable=validate_gcs_data,
    op_kwargs={
        'bucket_name': GCS_BUCKET,
        'file_path': GCS_FILE_PATH,
        'schema': SCHEMA
    },
    dag=dag,
)

# Define the GCS to BigQuery task
gcs_to_bq_task = GCSToBigQueryOperator(
    task_id="gcs_to_bq",
    bucket=GCS_BUCKET,
    source_objects=[GCS_FILE_PATH],
    destination_project_dataset_table="cycling-pipeline.cycling_dataset.central",
    schema_fields=[
        {"name": "Year", "type": "STRING", "mode": "NULLABLE"},
        {"name": "UnqID", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Weather", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Time", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Day", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Round", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Dir", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Path", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Mode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Count", "type": "STRING", "mode": "NULLABLE"}
    ],
    write_disposition="WRITE_TRUNCATE",  # Use WRITE_TRUNCATE to replace data
    skip_leading_rows=1,  # Skip header row if necessary
    source_format='CSV',
    dag=dag,
)

# Task dependencies
validate_data_task >> gcs_to_bq_task

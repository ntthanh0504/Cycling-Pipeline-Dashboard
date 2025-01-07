import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.kafka_client.py_consumer import main as consume_messages

# Default arguments for the DAG
default_args = {
    "owner": "viannTH",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

# DAG definition
with DAG(
    dag_id="kafka_to_mongodb_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task: Consume messages from Kafka and save to MongoDB
    consume_kafka_task = PythonOperator(
        task_id="consume_kafka_messages",
        python_callable=consume_messages,
        retries=3,
        retry_delay=timedelta(seconds=10),
        on_failure_callback=lambda context: print(
            f"Failed Task: {context['task_instance_key_str']}"
        ),
    )

    consume_kafka_task

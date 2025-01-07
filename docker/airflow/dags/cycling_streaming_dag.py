import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.baseoperator import chain
from src.kafka_client.producer import produce_records, create_topics
from src.kafka_client.py_consumer import main as mongo_consumer
from src.managers.api_crawling_manager import store_cycling_files, fetch_files_data
from src.helpers.mongo_helpers import insert_data

# Utility function to handle failures
def on_failure(context):
    print(f"Failed Task: {context['task_instance_key_str']}")


# Utility to create PythonOperator
def create_python_task(
    task_id, python_callable, op_args=None, retries=3, retry_delay=10, **kwargs
):
    return PythonOperator(
        task_id=task_id,
        python_callable=python_callable,
        op_args=op_args or [],
        retries=retries,
        retry_delay=timedelta(seconds=retry_delay),
        on_failure_callback=on_failure,
        **kwargs,
    )


def _fetch_files_data(place: str, **kwargs):
    data = fetch_files_data(place)
    kwargs["ti"].xcom_push(key="result_of_fetch_files_data", value=data)


def _produce_records(topic, **kwargs):
    data = kwargs["ti"].xcom_pull(key="result_of_fetch_files_data")
    print(f"Producing records to topic {topic}. Total records: {len(data)}")
    produce_records(topic, data)

def _staging_records(topic, **kwargs):
    data = kwargs["ti"].xcom_pull(key="result_of_fetch_files_data")
    print(f"Staging records. Total records: {len(data)}")
    insert_data("staging", topic, data)
    
default_args = {
    "owner": "viannTH",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="kafka_producer_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
) as dag:
    # task_start = BashOperator(task_id="start", bash_command="date")

    store_cycling_files_task = create_python_task(
        task_id="store_cycling_files",
        python_callable=store_cycling_files,
        op_args=["metadata", "files"],
    )

    # Fetch and Produce Tasks
    fetch_tasks = {
        "central": create_python_task(
            task_id="fetch_central_data",
            python_callable=_fetch_files_data,
            op_args=["Central"],
        ),
        "inner": create_python_task(
            task_id="fetch_inner_data",
            python_callable=_fetch_files_data,
            op_args=["Inner"],
        ),
    }

    create_topics_task = create_python_task(
        task_id="create_topics", python_callable=create_topics
    )

    # Produce Records Tasks
    produce_tasks = {
        "central": create_python_task(
            task_id="produce_central_records",
            python_callable=_produce_records,
            op_args=["Central"],
        ),
        "inner": create_python_task(
            task_id="produce_inner_records",
            python_callable=_produce_records,
            op_args=["Inner"],
        ),
    }

    mongo_consumer_task = create_python_task(
        task_id="mongo_consumer", python_callable=mongo_consumer
    )
    
    spark_consumer_task = SparkSubmitOperator(
        task_id="data_quality_consumer",
        application="/opt/airflow/dags/src/spark/spark_streaming.py",
        conn_id="spark_local",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.kafka:kafka-clients:3.3.1",
        repositories="https://repo1.maven.org/maven2/,https://repos.spark-packages.org/",
        verbose=True,
    )
    
    bridge_task = BashOperator(task_id="bridge", bash_command="date")
    
    store_cycling_files_task >> [fetch_tasks["central"], fetch_tasks["inner"], create_topics_task]

    # # # Task Group for produce records
    # with TaskGroup("produce_records") as produce_records_group:
    fetch_tasks["central"] >> produce_tasks["central"]
    fetch_tasks["inner"] >> produce_tasks["inner"]

    # Use chain to set dependencies between produce tasks and consumer tasks
    [produce_tasks["central"], produce_tasks["inner"]] >> bridge_task >> [mongo_consumer_task, spark_consumer_task]

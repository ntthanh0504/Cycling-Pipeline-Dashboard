import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default arguments for the DAG
default_args = {
    "owner": "viannTH",
    "start_date": days_ago(1),  # Improved start date handling
    "retries": 1,
    "retry_delay": timedelta(
        seconds=10
    ),  # Slightly increased retry delay to allow recovery
}

# DAG definition
with DAG(
    dag_id="kafka_spark_DAG",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,  # Ensure only one DAG run at a time
) as dag:

    # Task 1: Run the Kafka Producer
    # kafka_stream_task = PythonOperator(
    #     task_id="kafka_producer",
    #     python_callable=kafka_producer,
    #     retries=3,
    #     retry_delay=timedelta(seconds=10),  # Add retries and delay for task robustness
    #     on_failure_callback=lambda context: print(
    #         f"Failed Task: {context['task_instance_key_str']}"
    #     ),  # Add failure logging
    # )

    # Task 2: Run the PySpark Consumer using DockerOperator
    spark_stream_task = SparkSubmitOperator(
        task_id="pyspark_consumer",
        application="/opt/airflow/dags/src/spark/spark_streaming.py",
        conn_id="spark_local",
        # jars="/opt/airflow/shared-jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,/opt/airflow/shared-jars/kafka-clients-3.4.0.jar",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.kafka:kafka-clients:3.3.1",
        repositories="https://repo1.maven.org/maven2/,https://repos.spark-packages.org/",
        verbose=True,
    )
    # spark_stream_task = DockerOperator(
    #     task_id="pyspark_consumer",
    #     image="cycling-pipeline/spark:latest",
    #     api_version="auto",
    #     auto_remove=True,
    #     command=(
    #         "./bin/spark-submit "
    #         "--master local[*] "
    #         "--jars /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar "
    #         "--packages org.postgresql:postgresql:42.2.23,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 "
    #         "./spark_streaming.py"
    #     ),
    #     environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
    #     network_mode="data-flow-net",
    #     docker_url="tcp://docker-proxy:2375",  # Point to the docker-proxy on port 2375
    #     retries=3,  # Retry the Docker task if it fails
    #     retry_delay=timedelta(seconds=10),
    #     on_failure_callback=lambda context: print(f"Failed Task: {context['task_instance_key_str']}"),
    #     trigger_rule="all_success",  # Only run if kafka_stream_task succeeds
    # )

    # # Define task dependencies
    # kafka_stream_task >> spark_stream_task
    spark_stream_task

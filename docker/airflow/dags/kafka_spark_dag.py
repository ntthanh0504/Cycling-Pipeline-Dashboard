from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
# from src.kafka_client.stream_data import main as kafka_main
from src.kafka_client.producer import main as kafka_producer
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "start_date": datetime.today() - timedelta(days=1),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="kafka_spark_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    kafka_stream_task = PythonOperator(
        task_id="kafka_producer",
        python_callable=kafka_producer,
    )

    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer",
        image="cycling-pipeline/spark:latest",
        api_version="auto",
        auto_remove=True,
        command="./bin/spark-submit --master local[*] --jars /opt/bitnami/spark/jars/gcs-connector-hadoop2-latest.jar --packages org.postgresql:postgresql:42.2.23,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 ./spark_streaming.py",
        docker_url='tcp://docker-proxy:2375',
        environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
        network_mode="airflow-kafka",
        mounts=[
            Mount(source='/home/thanh_linux/.google/credentials',
                  target='/opt/bitnami/spark/.google/credentials', type='bind')
        ],
    )

    kafka_stream_task >> spark_stream_task
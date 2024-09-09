from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
    
from pyspark.sql import SparkSession
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s")
def test():
    try:
        if not os.path.exists("/opt/airflow/config/credential_key.json"):
            logging.error("Google credentials file not found")
            raise FileNotFoundError("Google credentials file not found")
        
        if not os.path.exists("/opt/airflow/config/gcs-connector-hadoop3-latest.jar"):
            logging.error("GCS connector JAR file not found")
            raise FileNotFoundError("GCS connector JAR file not found")
        
        spark = SparkSession.builder \
            .appName("Spark to BigQuery Test") \
            .master('spark://spark-master:7077') \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2") \
            .config("spark.jars", "/opt/airflow/config/gcs-connector-hadoop3-latest.jar") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/config/credential_key.json") \
            .config("spark.hadoop.google.cloud.project.id", "cycling-pipeline") \
            .getOrCreate()
        logging.info("Spark session created successfully")

        # Create a sample DataFrame
        data = [("John", 28), ("Jane", 35), ("Jake", 42)]
        columns = ["Name", "Age"]
        df = spark.createDataFrame(data, columns)

        # Write the DataFrame to BigQuery
        df.write.format("bigquery") \
            .option("table", "cycling-pipeline.cycling_dataset.test") \
            .option("temporaryGcsBucket", "ntt_cycling_bucket") \
            .option("parentProject", "cycling-pipeline") \
            .mode("append") \
            .save()
        logging.info("Data written to BigQuery successfully")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    
    
# Define default arguments for the DAG
default_args = {
    'start_date': datetime(2023, 9, 5),
    'catchup': False,
}

# Define the DAG
with DAG(
    dag_id='test_spark_bigquery_connection',
    default_args=default_args,
    schedule_interval=None,  # You can adjust this as per your need
    description='Test DAG for Spark and BigQuery connection',
) as dag:
    #Define the PythonOperator
    spark_stream_task = DockerOperator(
        task_id="pyspark_consumer",
        image="cycling-pipeline/spark:latest",
        api_version="auto",
        auto_remove=True,
        command="./bin/spark-submit --master local[*] --jars /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar ./test_connection.py",
        docker_url='tcp://docker-proxy:2375',
        environment={'SPARK_LOCAL_HOSTNAME': 'localhost'},
        network_mode="data-flow-net",
        mounts=[
            Mount(source='/home/viann/.google/credentials',
                    target='/opt/bitnami/spark/.google/credentials', type='bind')
        ],
    )
    spark_stream_task

# --packages org.postgresql:postgresql:42.2.23,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1


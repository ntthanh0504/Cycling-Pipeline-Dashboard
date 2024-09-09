from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark import SparkConf
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd
import os
import logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s")

default_args = {
    'owner': 'viannTH',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}


def create_spark_session():
    # Set the path for Google credentials
    google_credentials_path = os.getenv(
        "GOOGLE_CREDENTIALS_PATH", "/opt/airflow/config/credential_key.json")

    if not os.path.exists(google_credentials_path):
        logging.error(f"Google credentials file not found at {google_credentials_path}")
        raise FileNotFoundError(f"Google credentials file not found at {google_credentials_path}")
    else:
        logging.info(f"Google credentials file found at {google_credentials_path}")
        
    conf = SparkConf().setAppName('Get streaming data to GCS') \
            .setMaster("spark://spark-master:7077") \
            .set("spark.executor.memory", "512m") \
            .set("spark.jars.packages",
                 "org.postgresql:postgresql:42.2.23,"  # PostgreSQL connector
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"  # Kafka connector
                 "com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.2,"  # GCS connector
                 "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2") \
            .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
            .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")\
            .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials_path)\
            .set("parentProject", "cycling-pipeline")\
            .set("temporaryGcsBucket", "ntt_cycling_bucket")
    try:
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        logging.info("Spark session created successfully")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise e


def test_spark_connection():
    jdbc_url = "jdbc:postgresql://pgdatabase:5432/cycling_data"
    properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    spark = create_spark_session()
    logging.info("Testing Spark connection")
    spark.stop()
    # try:
    #     # Example: Reading data from a PostgreSQL table
    #     df = spark.read.jdbc(url=jdbc_url, table="scooter",
    #                          properties=properties)
    #     logging.info(f"Spark connection successful: {len(df.collect())} rows returned")
    #     spark.stop()
    # except Exception as e:
    #     logging.error(f"Error testing Spark connection: {e}")
    #     raise e


with DAG(
    dag_id="test_spark_DAG",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    test_spark_task = PythonOperator(
        task_id="test_spark_connection",
        python_callable=test_spark_connection,
    )
    test_spark_task

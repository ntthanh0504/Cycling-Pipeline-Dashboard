from pyspark import SparkConf
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from src.constants import PostgresConfig, CyclingDataAPI, KafkaConfig
import logging
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


SCHEMA = T.StructType([
    T.StructField("Year", T.StringType(), True),
    T.StructField("UnqID", T.StringType(), True),
    T.StructField("Date", T.StringType(), True),
    T.StructField("Weather", T.StringType(), True),
    T.StructField("Time", T.StringType(), True),
    T.StructField("Day", T.StringType(), True),
    T.StructField("Round", T.StringType(), True),
    T.StructField("Dir", T.StringType(), True),
    T.StructField("Path", T.StringType(), True),
    T.StructField("Mode", T.StringType(), True),
    T.StructField("Count", T.StringType(), True)
])

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)


def create_table(spark: SparkSession, table: str) -> None:
    """
    Create a table in Postgres.

    Args:
        spark (SparkSession): Spark session object 
        table (str): Table name
    """
    
    df = spark.createDataFrame([], SCHEMA)
    df.write.jdbc(PostgresConfig.URL, table, "overwrite", properties=PostgresConfig.PROPERTIES)
    logging.info(f"Table {table} created successfully in Postgres")


def create_spark_session():
    """
    Create a Spark session.

    Raises:
        FileNotFoundError: If Google credentials file is not found

    Returns:
        _type_: SparkSession object
    """
    
    try:
        google_credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "/opt/bitnami/spark/.google/credentials/cycling-pipeline-dashboard-33b9b532c7a9.json")

        if not os.path.exists(google_credentials_path):
            logging.error(f"Google credentials file not found at {google_credentials_path}")
            raise FileNotFoundError(f"Google credentials file not found at {google_credentials_path}")

        conf = SparkConf().setAppName('Get streaming data to GCS').setMaster('local')\
            .set("spark.jars.packages", "org.postgresql:postgresql:42.2.23,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")\
            .set("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop2-latest.jar, /opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.24.2.jar")\
            .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")\
            .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")\
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")\
            .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials_path)\
            .set("parentProject", "cycling-pipeline-dashboard")\
            .set("temporaryGcsBucket", "cycling_pipeline_bucket")\

        logging.info("Attempting to create Spark session...")
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        logging.info("Spark session created successfully")

        return spark
    except Exception as e:
        logging.warning(f"Failed to create Spark session: {e}")
        raise


def read_from_kafka(consume_topic: str, spark: SparkSession):
    """
    Read data from Kafka.

    Args:
        consume_topic (str): Kafka topic to consume data from
        spark (SparkSession): Spark session object

    Returns:
        _type_: DataFrame
    """
    
    try:
        df = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "kafka:9092")\
            .option("subscribe", consume_topic)\
            .option("startingOffsets", "earliest")\
            .option("checkpointLocation", "/tmp/checkpoint")\
            .load()
        logging.info("Data read from Kafka successfully")
    except Exception as e:
        logging.warning(f"Failed to read data from Kafka: {e}")
        raise
    return df


def parse_data_from_kafka(df, schema: T.StructType):
    """
    Parse data from Kafka.

    Args:
        df (_type_): DataFrame
        schema (T.StructType): Schema to parse data

    Returns:
        _type_: DataFrame
    """
    
    assert df.isStreaming is True, "DataFrame is not streaming"
    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(F.from_json(F.col("value"), schema).alias(
        "data")).select("data.*")
    logging.info("Data parsed successfully")
    return df


def sink_to_postgres(df, table: str, properties: dict) -> None:
    """
    Sink data to Postgres.

    Args:
        df (_type_): DataFrame
        table (str): Table name
        properties (dict): Postgres properties including user, password, and driver
    """
    
    try:
        df.writeStream\
            .foreachBatch(lambda batch_df, _: batch_df.write.jdbc(PostgresConfig.URL, table, "append", properties=properties))\
            .trigger(once=True)\
            .start()\
            .awaitTermination()
    except Exception as e:
        logging.warning(f"Failed to sink data to Postgres: {e}")


def sink_to_gcs(df, bucket: str, path: str, mode: str = "overwrite") -> None:
    """
    Sink data to GCS.

    Args:
        df (_type_): DataFrame
        bucket (str): GCS bucket name
        path (str): GCS path
        mode (str, optional): Output mode. Defaults to "overwrite".
    """
    
    try:
        output_path = f"gs://{bucket}/{path}/"
        checkpoint_path = f"/tmp/checkpoint/{bucket}/{path}"

        df.writeStream\
            .format("csv")\
            .option("path", output_path)\
            .option("checkpointLocation", checkpoint_path)\
            .trigger(once=True)\
            .outputMode(mode)\
            .start()\
            .awaitTermination()
    except Exception as e:
        logging.warning(f"Failed to sink data to GCS: {e}")

def sink_to_bigquery(df, table: str, mode: str = "append") -> None:
    try:
        df.writeStream\
            .foreachBatch(lambda batch_df, _: batch_df.write.format("bigquery")\
                .option("table", table)\
                .option("temporaryGcsBucket", "cycling_pipeline_bucket")\
                .option("parentProject", "cycling-pipeline-dashboard")\
                .mode(mode)\
                .save())\
            .trigger(once=True)\
            .outputMode(mode)\
            .start()\
            .awaitTermination()
    except Exception as e:
        logging.warning(f"Failed to sink data to BigQuery: {e}")

def main():
    spark = create_spark_session()
    # create_table(spark, "cycling_table")
    for year in CyclingDataAPI.YEARS:
        topic = f"cycling-pipeline-{year}"
        df = read_from_kafka(topic, spark)
        df = parse_data_from_kafka(df, SCHEMA)
        # sink_to_postgres(df, "cycling_table", POSTGRES_PROPERTIES)
        # sink_to_gcs(df, "cycling_pipeline_bucket", f"cycling-data/{year}", "append")
        sink_to_bigquery(df, f"cycling-pipeline-dashboard:cycling_dataset.{year}")


if __name__ == "__main__":
    main()

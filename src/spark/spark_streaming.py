from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from src.config.constants import PostgresConfig, CyclingDataAPI, KafkaConfig
import logging
import os

# Define schema for the incoming data
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

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")


def create_spark_session():
    """Create a Spark session."""
    try:
        google_credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "/opt/bitnami/spark/credentials.json")
        if not os.path.exists(google_credentials_path):
            raise FileNotFoundError(f"Google credentials file not found at {google_credentials_path}")

        spark = SparkSession.builder \
            .appName("Cycling Data Pipeline") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2") \
            .config("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials_path) \
            .config("spark.hadoop.google.cloud.project.id", "cycling-pipeline") \
            .getOrCreate()

        logging.info("Spark session created successfully")
        return spark

    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        raise


def read_from_kafka(spark, topic):
    """Read data from Kafka."""
    try:
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .load()

        logging.info(f"Data read from Kafka topic {topic} successfully")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from Kafka: {e}")
        raise


def parse_data_from_kafka(df, schema):
    """Parse Kafka data based on schema."""
    try:
        df = df.selectExpr("CAST(value AS STRING)")
        df = df.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")
        logging.info("Data parsed successfully from Kafka")
        return df
    except Exception as e:
        logging.error(f"Error parsing data from Kafka: {e}")
        raise


def write_to_postgres(df, table):
    """Sink data to Postgres."""
    try:
        df.writeStream.foreachBatch(lambda batch_df, _: batch_df.write.jdbc(
            PostgresConfig.URL, table, "append", properties=PostgresConfig.PROPERTIES)) \
            .trigger(once=True) \
            .start().awaitTermination()

        logging.info(f"Data successfully written to Postgres table: {table}")
    except Exception as e:
        logging.error(f"Failed to write data to Postgres: {e}")
        raise


def write_to_gcs(df, bucket, path, mode="overwrite"):
    """Sink data to Google Cloud Storage (GCS)."""
    try:
        output_path = f"gs://{bucket}/{path}/"
        checkpoint_path = f"/tmp/checkpoint/{bucket}/{path}"

        df.writeStream \
            .format("csv") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(once=True) \
            .outputMode(mode) \
            .start().awaitTermination()

        logging.info(f"Data successfully written to GCS bucket: {bucket}/{path}")
    except Exception as e:
        logging.error(f"Failed to write data to GCS: {e}")
        raise


def write_to_bigquery(df, table, mode="append"):
    """Sink data to BigQuery."""
    try:
        df.writeStream \
            .foreachBatch(lambda batch_df, _: batch_df.write.format("bigquery")
                          .option("table", table)
                          .option("temporaryGcsBucket", "ntt_cycling_bucket")
                          .option("parentProject", "cycling-pipeline")
                          .mode(mode)
                          .save()) \
            .trigger(once=True) \
            .outputMode(mode) \
            .start().awaitTermination()

        logging.info(f"Data successfully written to BigQuery table: {table}")
    except Exception as e:
        logging.error(f"Failed to write data to BigQuery: {e}")
        raise


def main():
    try:
        spark = create_spark_session()
        for topic in KafkaConfig.TOPIC:
            df = read_from_kafka(spark, topic)
            parsed_df = parse_data_from_kafka(df, SCHEMA)
            write_to_gcs(parsed_df, "ntt_cycling_bucket", f"data/{topic}", mode="append")
        # write_to_bigquery(parsed_df, f"cycling-pipeline:cycling_dataset.{year}")

    except Exception as e:
        logging.error(f"Error in main pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()

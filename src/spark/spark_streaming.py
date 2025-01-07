from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf

# from delta import *
# from delta.tables import *
import sys, os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from src import constants as cfg
from src.data_quality.data_quality import DataQuality
from src.utils.utils import create_df_from_dq_results, get_violated_rows
from src.helpers.mongo_helpers import upsert_data, fetch_data, check_collection_exists, insert_data

# from config.constants import PostgresConfig, CyclingDataAPI, KafkaConfig
import logging
import os

# Define schema for the incoming data
SCHEMA = T.StructType(
    [
        T.StructField("Wave", T.StringType(), True),
        T.StructField("SiteID", T.StringType(), True),
        T.StructField("Date", T.StringType(), True),
        T.StructField("Weather", T.StringType(), True),
        T.StructField("Time", T.StringType(), True),
        T.StructField("Day", T.StringType(), True),
        T.StructField("Round", T.StringType(), True),
        T.StructField("Direction", T.StringType(), True),
        T.StructField("Path", T.StringType(), True),
        T.StructField("Mode", T.StringType(), True),
        T.StructField("Count", T.StringType(), True),
    ]
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")




def create_spark_session():
    """Create a Spark session."""
    try:
        # google_credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "src/spark/credentials.json")
        # if not os.path.exists(google_credentials_path):
        #     raise FileNotFoundError(f"Google credentials file not found at {google_credentials_path}")
        # jars = 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2,io.delta:delta-spark_2.12:3.2.0,io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:3.2.0'
        jars = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3,org.apache.kafka:kafka-clients:3.3.1"
        conf = (
            SparkConf()
            .setAppName("CyclingDataPipeline")
            .set("spark.jars.packages", jars)
        )
        # .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        # .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

        # .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials_path) \
        # .set("spark.hadoop.google.cloud.project.id", "cycling-pipeline") \
        # .set("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar")

        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        logging.info("Spark session created successfully")
        return spark

    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        raise


def read_from_kafka(spark, topic):
    """Read data from Kafka."""
    try:
        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("checkpointLocation", "/tmp/checkpoint")
            .load()
        )

        logging.info(f"Data read from Kafka topic {topic} successfully")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from Kafka: {e}")
        raise


def parse_data_from_kafka(df, schema):
    """Parse Kafka data based on schema."""
    try:
        df = df.withColumn("data", F.from_json(F.col("value").cast("string"), schema)).select("data.*")
        logging.info("Data parsed successfully from Kafka")
        return df
    except Exception as e:
        logging.error(f"Error parsing data from Kafka: {e}")
        raise


def write_to_postgres(df, table):
    """Sink data to Postgres."""
    try:
        df.writeStream.foreachBatch(
            lambda batch_df, _: batch_df.write.jdbc(
                cfg.PostgresConfig.URL,
                table,
                "append",
                properties=cfg.PostgresConfig.PROPERTIES,
            )
        ).trigger(once=True).start().awaitTermination()

        logging.info(f"Data successfully written to Postgres table: {table}")
    except Exception as e:
        logging.error(f"Failed to write data to Postgres: {e}")
        raise


def write_to_gcs(df, bucket, path, mode="overwrite"):
    """Sink data to Google Cloud Storage (GCS)."""
    try:
        output_path = f"gs://{bucket}/{path}/"
        checkpoint_path = f"/tmp/checkpoint/{bucket}/{path}"

        df.writeStream.format("csv").option("path", output_path).option(
            "checkpointLocation", checkpoint_path
        ).trigger(once=True).outputMode(mode).start().awaitTermination()

        logging.info(f"Data successfully written to GCS bucket: {bucket}/{path}")
    except Exception as e:
        logging.error(f"Failed to write data to GCS: {e}")
        raise


def write_to_bigquery(df, table, mode="append"):
    """Sink data to BigQuery."""
    try:
        df.writeStream.foreachBatch(
            lambda batch_df, _: batch_df.write.format("bigquery")
            .option("table", table)
            .option("temporaryGcsBucket", "ntt_cycling_bucket")
            .option("parentProject", "cycling-pipeline")
            .mode(mode)
            .save()
        ).trigger(once=True).outputMode(mode).start().awaitTermination()

        logging.info(f"Data successfully written to BigQuery table: {table}")
    except Exception as e:
        logging.error(f"Failed to write data to BigQuery: {e}")
        raise



def quality_check(spark, df):
    # Ghi dữ liệu streaming ra file sink
    query = df.writeStream \
        .format("parquet") \
        .option("path", "/tmp/stream_output") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .outputMode("append") \
        .start()

    query.awaitTermination(timeout=30)  # Chạy streaming trong 30 giây (hoặc bất kỳ khoảng thời gian nào bạn muốn)

    # Đọc lại dữ liệu dưới dạng batch DataFrame
    batch_df = spark.read.schema(SCHEMA).parquet("/tmp/stream_output")
    batch_df.show()
    # batch_dataframes = []  # This will hold batch DataFrames for each micro-batch

    # # Define function to process each micro-batch
    # def process_batch(batch_df, batch_id):
    #     # Append the batch DataFrame to a list (for example, collecting all batches)
    #     batch_dataframes.append(batch_df.collect())  # Collect all data from the batch

    # # Use foreachBatch to process the streaming DataFrame
    # query = df.writeStream.foreachBatch(process_batch).outputMode("append").start()

    # query.awaitTermination(30)  # Run for 30 seconds (adjust as needed)

    # # After termination, combine all collected batch data (if applicable)
    # if batch_dataframes:
    #     combined_batch_df = spark.createDataFrame(sum(batch_dataframes, []))
    #     combined_batch_df.show()

    dq = DataQuality(batch_df, "/opt/airflow/dags/src/config/test.json")
    dq_results = dq.run_test()

    dq_df = create_df_from_dq_results(spark, dq_results)
    dq_df.show()

    violated_dfs = get_violated_rows(batch_df, dq_results)
    for df in violated_dfs:
        non_violated_df = batch_df.subtract(df)
    return non_violated_df, violated_dfs

def store_audit_table(spark, dfs, topic):
    for df in dfs:
        data = df.toPandas().to_dict(orient="records")
        for row in data:
            row["row_id"] = row["row_id"].item()
        insert_data("metadata", f"{topic}_audit", df.toPandas().to_dict(orient="records"))

def store_dq_logs(spark, dq_results, topic):
    pass

def main():
    try:
        spark = create_spark_session()
        for topic in cfg.KafkaConfig.TOPIC:
            df = read_from_kafka(spark, topic)
            parsed_df = parse_data_from_kafka(df, SCHEMA)
            # logging.info(f"Length of parsed_df: {parsed_df.count()}")
            # staging_to_delta(parsed_df, topic)
            # staging_to_mongodb(parsed_df, topic, spark)
            logging.info(f"Schema of parsed_df: {parsed_df.printSchema()}")
            quality_check(spark, parsed_df)
            break
            # staging_to_delta(parsed_df, topic)
            # write_to_gcs(parsed_df, "ntt_cycling_bucket", f"data/{topic}", mode="append")
        # write_to_bigquery(parsed_df, f"cycling-pipeline:cycling_dataset.{year}")

    except Exception as e:
        logging.error(f"Error in main pipeline execution: {e}")
        raise


if __name__ == "__main__":
    main()

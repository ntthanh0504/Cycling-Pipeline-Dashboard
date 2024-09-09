from pyspark.sql import SparkSession

import os
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s")

def main():
    try:
        google_credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", "/opt/bitnami/spark/credentials.json")

        if not os.path.exists(google_credentials_path):
            logging.error(f"Google credentials file not found at {google_credentials_path}")
            raise FileNotFoundError(f"Google credentials file not found at {google_credentials_path}")
        
        spark = SparkSession.builder \
            .appName("Spark to BigQuery Test") \
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2") \
            .config("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", google_credentials_path) \
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
    
if __name__ == "__main__":
    main()
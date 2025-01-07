import time
from prometheus_client import start_http_server, Summary, Counter, Gauge
from pyspark.sql import SparkSession

# Create a few Prometheus metrics
job_duration = Summary('spark_job_duration_seconds', 'Time spent processing Spark job')
job_success = Counter('spark_job_success', 'Count of successful Spark jobs')
job_error = Counter('spark_job_error', 'Count of failed Spark jobs')
memory_usage = Gauge('spark_job_memory_usage_bytes', 'Memory used by Spark job')

# Start the Prometheus server to expose metrics
start_http_server(8000)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Prometheus Spark Monitoring") \
    .config("spark.metrics.conf", "conf/metrics.properties") \
    .config("spark.some.config.option", "config-value") \
    .getOrCreate()

# Example Spark job
@job_duration.time()
def run_spark_job():
    try:
        # Simulate a Spark job
        df = spark.read.csv('external_data/raw/1 Monitoring locations.csv', header=True, inferSchema=True)  # Replace with your data
        df.createOrReplaceTempView("data")
        result = spark.sql("SELECT COUNT(*) FROM data")  # Sample query
        result.show()

        job_success.inc()  # Increment job success counter
    except Exception as e:
        job_error.inc()  # Increment job error counter
        print(f"Job failed: {e}")

    # Monitor memory usage
    # (This is just a placeholder as actual memory monitoring would need integration with Spark or OS-level metrics)
    memory_usage.set(1024 * 1024 * 1024 * 2)  # Set memory usage in bytes (for example, 2GB)

if __name__ == "__main__":
    while True:
        run_spark_job()
        time.sleep(60)  # Run the job every 60 seconds

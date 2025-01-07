from pyspark.sql import SparkSession
from pyspark import SparkConf

spark = SparkSession.builder \
    .appName("kafkaDemo") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Test Kafka connection
try:
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094") \
        .option("subscribe", "test") \
        .option("startingOffsets", "earliest") \
        .load()
    print("Connected to Kafka successfully")
except Exception as e:
    print(f"Failed to connect to Kafka: {e}")
    raise
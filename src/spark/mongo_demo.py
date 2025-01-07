from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# MongoDB connection details
username = "root"  # Replace with your MongoDB username
password = "123"  # Replace with your MongoDB password
database = "test"  # Replace with your MongoDB database name
collection = "thanh"  # Replace with your MongoDB collection name
host = "mongodb"  # MongoDB host (use MongoDB Atlas URI if in the cloud)
port = 27017  # Default MongoDB port
auth_db = "admin"  # Authentication database, usually "admin"

# MongoDB URI with authentication database
uri = f"mongodb://{username}:{password}@{host}:{port}/{database}.{collection}?authSource={auth_db}"

# Sample data to insert into MongoDB
sample_data = [
    (1, "Alice", 30),
    (2, "Bob", 22),
    (3, "Charlie", 35)
]

# Define schema for the sample data
schema = StructType([
    StructField("_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MongoDBExampleWithAuth") \
    .config("spark.mongodb.read.connection.uri", uri) \
    .config("spark.mongodb.write.connection.uri", uri) \
    .getOrCreate()

# Create DataFrame from sample data
df = spark.createDataFrame(sample_data, schema)

# Write sample data to MongoDB
df.write.format("mongodb").mode("overwrite").save()
print("Sample data written to MongoDB.")

# Read data from MongoDB
df = spark.read.format("mongodb").load()

# Show the data
print("Data from MongoDB:")
df.show()

# Stop the Spark session
spark.stop()
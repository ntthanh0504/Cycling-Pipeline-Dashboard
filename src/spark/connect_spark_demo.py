from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataFrame Example") \
    .getOrCreate()

# Create a DataFrame from a list of tuples
data = [("Alice", 28), ("Bob", 24), ("Cathy", 29)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Filter rows where age > 25
df.filter(df.Age > 25).show()

# Stop SparkSession
spark.stop()

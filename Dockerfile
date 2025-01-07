# # Use the official Apache Spark image with Python 3
# FROM bitnami/spark:latest

# # Set environment variables for PySpark (update SPARK_HOME based on the Bitnami Spark structure)
# ENV SPARK_HOME /opt/bitnami/spark
# ENV PATH $SPARK_HOME/bin:$PATH

# # Install additional dependencies if needed
# RUN pip install --no-cache-dir pandas

# # Create working directory
# WORKDIR /app

# # Copy all necessary files to the Docker container
# COPY src /app/src

# # Ensure dependencies are available
# RUN pip install -r /app/src/requirements.txt

# # Set default command to run your Spark job
# CMD ["/opt/bitnami/spark/bin/spark-submit", "/app/src/main.py"]

# Use the Delta Lake Docker image as the base
FROM deltaio/delta-docker:latest

# Set up environment variables
ENV PYSPARK_PYTHON=/usr/bin/python3 \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Install additional Python dependencies
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install pyspark delta-spark

# Create a working directory
WORKDIR /app

# Copy all necessary files to the Docker container
COPY src /app/src

# Define the entry point for the container
ENTRYPOINT ["python3", "/app/src/main.py"]



FROM bitnami/spark:3.4.1

# Set working directory for Spark
WORKDIR /opt/bitnami/spark

# Install required Python packages (py4j already comes with Spark, but it's here for ensuring compatibility)
RUN pip install py4j

# Copy Python scripts from src/spark
COPY ./src/spark/test_connection.py ./test_connection.py
COPY ./src/spark/spark_streaming.py ./spark_streaming.py

# Copy constants configuration from src/config
COPY ./src/config/constants.py ./src/config/constants.py

# Copy JAR dependencies into Spark's jars folder
COPY ./docker/spark/jars/gcs-connector-hadoop3-latest.jar /opt/bitnami/spark/jars/
COPY ./docker/spark/jars/spark-bigquery-with-dependencies_2.12-0.24.2.jar /opt/bitnami/spark/jars/

# Copy credentials for BigQuery or other integrations
COPY ./docker/spark/gcp-credentials/credentials.json /opt/bitnami/spark/credentials.json
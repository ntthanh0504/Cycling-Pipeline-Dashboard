FROM bitnami/spark:3.4.1


WORKDIR /opt/bitnami/spark

RUN pip install py4j

COPY ./src/spark/spark_streaming.py ./spark_streaming.py
COPY ./src/constants.py ./src/constants.py
COPY ./src/gcs-connector-hadoop2-latest.jar /opt/bitnami/spark/jars/
COPY ./src/spark-bigquery-with-dependencies_2.12-0.24.2.jar /opt/bitnami/spark/jars/


ENV POSTGRES_DOCKER_USER=host.docker.internal
ARG POSTGRES_PASSWORD
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

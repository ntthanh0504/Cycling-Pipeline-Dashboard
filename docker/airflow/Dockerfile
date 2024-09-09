FROM apache/airflow:2.7.3

USER root

# Install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libffi-dev \
    apt-transport-https \
    gnupg2 \
    lsb-release \
    openjdk-11-jdk \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Set Airflow version
ARG AIRFLOW_VERSION=2.7.3

# Install Python packages
RUN pip install py4j
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" unidecode==1.3.7 pyspark==3.4.1 confluent_kafka

# Verify installations
RUN python --version
RUN airflow version

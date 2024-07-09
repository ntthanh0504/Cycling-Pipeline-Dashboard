# Project Setup Guide

This guide provides detailed instructions for setting up a robust data pipeline capable of handling both streaming and batch data processing. Depending on your specific data processing requirements, you can opt for:

- **Stream Processing**: Ideal for consuming data in real-time and storing it in a data lake for immediate analysis or future use.
- **Batch Processing**: Best suited for executing data processing tasks at scheduled intervals, such as hourly or daily, to handle large volumes of data efficiently.

## Technologies Overview

Here's a brief overview of the technologies and tools we'll be using to build our data pipeline:

- **Cloud Platform**: Google Cloud Platform (GCP) - A suite of cloud computing services that runs on the same infrastructure that Google uses internally for its end-user products.
- **Infrastructure as Code (IaC)**: Terraform - An open-source tool that allows you to define and provision a cloud infrastructure using a high-level configuration language.
- **Workflow Orchestration**: Apache Airflow - An open-source platform to programmatically author, schedule, and monitor workflows.
- **Data Warehouse**: Google BigQuery - A fully-managed, serverless data warehouse that enables scalable analysis over petabytes of data.
- **Batch Data Processing**: Apache Spark - An open-source, distributed computing system that provides an interface for programming entire clusters with implicit data parallelism and fault tolerance.
- **Stream Data Processing**: Apache Kafka - A distributed event streaming platform capable of handling trillions of events a day.

## Dashboard Creation

For creating dashboards, you can utilize tools such as Google Data Studio, Metabase, or any other Business Intelligence (BI) tool that fits your project requirements. It's important to ensure that the dashboard is accessible to your peers, especially if you opt for a tool not covered in the course. At a minimum, your dashboard should feature:

- **Categorical Data Distribution Graph**: A visual representation of the distribution of categorical data within your dataset.
- **Temporal Data Distribution Graph**: A timeline graph that displays how your data is distributed over a specific period.

## Detailed Setup Instructions

1. **Package Installation**:
   - Run `pip install -r requirements.txt` to install the necessary Python packages for your project. This step ensures that all dependencies are met for the tools and libraries you'll be using.

2. **Docker Setup**:
   - **For Kafka, Airflow, and Postgres & PgAdmin**: Execute the Dockerfiles located in their respective directories. This is crucial for scenarios where you're reading messages from Kafka topics and sending them to Postgres.
   - **For Terraform**: This step is necessary if your pipeline involves reading messages from Kafka topics and sending them to Google Cloud Storage (GCS) and BigQuery.

3. **Spark Docker Image**:
   - Build the Spark Docker image using the command: `docker build -f docker/spark/Dockerfile -t cycling-pipeline/spark:latest --build-arg POSTGRES_PASSWORD=postgres .`. This image is essential for batch processing tasks within your pipeline.

4. **Connector JAR Files**:
   - Ensure you have downloaded the following JAR files to establish connections to Google Cloud Storage and Google BigQuery:
     - `gcs-connector-hadoop2-latest.jar`
     - `spark-bigquery-with-dependencies_2.12-0.24.2.jar`

By following these detailed steps, you'll be well on your way to setting up a comprehensive data processing pipeline tailored to your project's needs.

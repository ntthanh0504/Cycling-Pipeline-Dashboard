# Variables
POSTGRES_PGADMIN_COMPOSE_FILE := docker/postgres-pgadmin/docker-compose.yaml
KAFKA_COMPOSE_FILE := docker/kafka/docker-compose.yaml
SPARK_COMPOSE_FILE := docker/spark/Dockerfile
AIRFLOW_COMPOSE_FILE := docker/airflow/docker-compose.yaml



# Targets
.PHONY: all build up down clean logs

all: build up

build:
	docker-compose -f $(POSTGRES_PGADMIN_COMPOSE_FILE) build
	docker-compose -f $(KAFKA_COMPOSE_FILE) build
	docker build -f $(SPARK_COMPOSE_FILE) -t cycling-pipeline/spark:latest .
	docker-compose -f $(AIRFLOW_COMPOSE_FILE) build

up:
	docker-compose -f $(POSTGRES_PGADMIN_COMPOSE_FILE) up -d
	docker-compose -f $(KAFKA_COMPOSE_FILE) up -d
	docker-compose -f $(AIRFLOW_COMPOSE_FILE) up -d

down:
	docker-compose -f $(POSTGRES_PGADMIN_COMPOSE_FILE) down
	docker-compose -f $(KAFKA_COMPOSE_FILE) down --volume
	docker-compose -f $(AIRFLOW_COMPOSE_FILE) down

clean: down
	docker-compose -f $(POSTGRES_PGADMIN_COMPOSE_FILE) rm -f
	docker-compose -f $(KAFKA_COMPOSE_FILE) rm -f
	docker-compose -f $(AIRFLOW_COMPOSE_FILE) rm -f

logs:
	docker-compose -f $(POSTGRES_PGADMIN_COMPOSE_FILE) logs -f
	docker-compose -f $(KAFKA_COMPOSE_FILE) logs -f
	docker-compose -f $(AIRFLOW_COMPOSE_FILE) logs -f

apply-change:
	docker build -f $(SPARK_COMPOSE_FILE) -t cycling-pipeline/spark:latest .

.PHONY: up down

RAY_COMPOSE_FILE := docker-compose.ray.yml
MLFLOW_COMPOSE_FILE := docker-compose.model-registry.yaml
MINIO_COMPOSE_FILE := docker-compose-minio.yml
COMPOSE_FILE := docker-compose.yml
up-network:
	docker network create confluent

up-minio:
	docker-compose -f $(MINIO_COMPOSE_FILE) up -d 

up-all:
	docker-compose up -d

load-data:
	python data_source/load_to_source.py

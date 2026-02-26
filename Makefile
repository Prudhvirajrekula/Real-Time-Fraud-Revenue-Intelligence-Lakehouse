# =============================================================================
# Fraud & Revenue Intelligence Platform - Makefile
# =============================================================================

.PHONY: help build up down restart logs clean reset \
        create-topics init-minio setup smoke-test \
        spark-bronze spark-silver spark-gold \
        dbt-run dbt-test dbt-docs \
        ml-train ml-serve \
        quality-check \
        test test-unit test-integration \
        format lint

SHELL := /bin/bash
.DEFAULT_GOAL := help

# Colors
RED    := \033[31m
GREEN  := \033[32m
YELLOW := \033[33m
BLUE   := \033[34m
PURPLE := \033[35m
CYAN   := \033[36m
RESET  := \033[0m

##@ General

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\n$(CYAN)Fraud & Revenue Intelligence Platform$(RESET)\n\nUsage:\n  make $(BLUE)<target>$(RESET)\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  $(BLUE)%-22s$(RESET) %s\n", $$1, $$2 } /^##@/ { printf "\n$(YELLOW)%s$(RESET)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Infrastructure

build: ## Build all Docker images
	@printf "$(GREEN)Building Docker images...$(RESET)\n"
	docker compose build

up: ## Start all services (detached)
	@printf "$(GREEN)Starting platform services...$(RESET)\n"
	docker compose up -d
	@printf "$(GREEN)Platform is starting. Run 'make status' to check.$(RESET)\n"

down: ## Stop all services
	@printf "$(YELLOW)Stopping platform services...$(RESET)\n"
	docker compose down

restart: ## Restart all services
	$(MAKE) down
	$(MAKE) up

status: ## Show service health status
	@printf "$(CYAN)Service Status:$(RESET)\n"
	@docker compose ps

logs: ## Tail logs for all services
	docker compose logs -f

logs-%: ## Tail logs for specific service (e.g. make logs-kafka)
	docker compose logs -f $*

##@ Setup & Initialization

setup: ## Full platform setup (build, start, initialize)
	@printf "$(GREEN)=====================================================$(RESET)\n"
	@printf "$(GREEN)  Fraud Intelligence Platform - Full Setup$(RESET)\n"
	@printf "$(GREEN)=====================================================$(RESET)\n"
	$(MAKE) build
	$(MAKE) up
	@printf "$(YELLOW)Waiting for services to be healthy...$(RESET)\n"
	@sleep 30
	$(MAKE) wait-healthy
	$(MAKE) create-topics
	$(MAKE) init-minio
	@printf "$(GREEN)Setup complete! Platform is ready.$(RESET)\n"
	$(MAKE) urls

wait-healthy: ## Wait until core services are healthy
	@printf "$(YELLOW)Waiting for Kafka...$(RESET)\n"
	@until docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server kafka:29092 > /dev/null 2>&1; do sleep 5; done
	@printf "$(GREEN)Kafka ready.$(RESET)\n"
	@printf "$(YELLOW)Waiting for MinIO...$(RESET)\n"
	@until docker compose exec -T minio curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; do sleep 5; done
	@printf "$(GREEN)MinIO ready.$(RESET)\n"
	@printf "$(YELLOW)Waiting for PostgreSQL...$(RESET)\n"
	@until docker compose exec -T postgres pg_isready -U platform_user -d fraud_platform > /dev/null 2>&1; do sleep 5; done
	@printf "$(GREEN)PostgreSQL ready.$(RESET)\n"

create-topics: ## Create Kafka topics
	@printf "$(GREEN)Creating Kafka topics...$(RESET)\n"
	@bash scripts/create_kafka_topics.sh

init-minio: ## Initialize MinIO buckets and structure
	@printf "$(GREEN)Initializing MinIO buckets...$(RESET)\n"
	@docker compose run --rm minio-init

##@ Data Processing

spark-bronze: ## Run Spark bronze layer streaming job
	@printf "$(GREEN)Starting Spark bronze streaming ingestion...$(RESET)\n"
	docker compose exec spark-master spark-submit \
		--master $(SPARK_MASTER_URL) \
		--packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		--conf "spark.hadoop.fs.s3a.endpoint=$(MINIO_ENDPOINT)" \
		--conf "spark.hadoop.fs.s3a.access.key=$(AWS_ACCESS_KEY_ID)" \
		--conf "spark.hadoop.fs.s3a.secret.key=$(AWS_SECRET_ACCESS_KEY)" \
		--conf "spark.hadoop.fs.s3a.path.style.access=true" \
		/opt/spark_jobs/bronze/ingest_stream.py

spark-silver: ## Run Spark silver layer transformation job
	@printf "$(GREEN)Running Spark silver transformations...$(RESET)\n"
	docker compose exec spark-master spark-submit \
		--master $(SPARK_MASTER_URL) \
		--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
		/opt/spark_jobs/silver/transform_transactions.py

spark-gold: ## Run Spark gold layer aggregation job
	@printf "$(GREEN)Running Spark gold aggregations...$(RESET)\n"
	docker compose exec spark-master spark-submit \
		--master $(SPARK_MASTER_URL) \
		--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 \
		--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
		/opt/spark_jobs/gold/revenue_aggregations.py

SPARK_MASTER_URL ?= spark://spark-master:7077
MINIO_ENDPOINT   ?= http://minio:9000
AWS_ACCESS_KEY_ID     ?= minioadmin
AWS_SECRET_ACCESS_KEY ?= minioadmin123

##@ dbt Transformations

dbt-run: ## Run all dbt models
	@printf "$(GREEN)Running dbt models...$(RESET)\n"
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run --profiles-dir ."

dbt-test: ## Run dbt tests
	@printf "$(GREEN)Running dbt tests...$(RESET)\n"
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test --profiles-dir ."

dbt-docs: ## Generate and serve dbt docs
	@printf "$(GREEN)Generating dbt docs...$(RESET)\n"
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8089 &"
	@printf "$(CYAN)dbt docs available at http://localhost:8089$(RESET)\n"

dbt-seed: ## Load dbt seed data
	docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt seed --profiles-dir ."

##@ ML Operations

ml-train: ## Train the fraud detection model
	@printf "$(GREEN)Training fraud detection model...$(RESET)\n"
	docker compose exec ml-server python /app/models/train.py

ml-serve: ## Check ML server health
	@curl -s http://localhost:8000/health | python3 -m json.tool

ml-predict: ## Test fraud prediction endpoint
	@curl -s -X POST http://localhost:8000/predict \
		-H "Content-Type: application/json" \
		-d '{"transaction_id": "test-001", "amount": 9999.99, "user_id": "user-123", "country": "XX"}' \
		| python3 -m json.tool

##@ Data Quality

quality-check: ## Run Great Expectations validation
	@printf "$(GREEN)Running data quality checks...$(RESET)\n"
	docker compose exec airflow-webserver python /opt/airflow/data_quality/validate.py

##@ Testing

test: ## Run all tests
	$(MAKE) test-unit
	$(MAKE) test-integration

test-unit: ## Run unit tests
	@printf "$(GREEN)Running unit tests...$(RESET)\n"
	@python -m pytest tests/unit/ -v --tb=short \
		--cov=data_generator --cov=ml \
		--cov-report=term-missing \
		--cov-report=html:coverage_report

test-integration: ## Run integration tests (requires running platform)
	@printf "$(GREEN)Running integration tests...$(RESET)\n"
	@python -m pytest tests/integration/ -v --tb=short -m integration

smoke-test: ## Run smoke tests to verify platform health
	@bash scripts/run_smoke_test.sh

##@ Code Quality

format: ## Format Python code with black + isort
	@printf "$(GREEN)Formatting code...$(RESET)\n"
	@black data_generator/ spark_jobs/ ml/ airflow/dags/ tests/
	@isort data_generator/ spark_jobs/ ml/ airflow/dags/ tests/

lint: ## Lint Python code with flake8 + mypy
	@printf "$(GREEN)Linting code...$(RESET)\n"
	@flake8 data_generator/ spark_jobs/ ml/ airflow/dags/ tests/ --max-line-length=120
	@mypy data_generator/ ml/ --ignore-missing-imports

##@ Monitoring

open-kafka-ui: ## Open Kafka UI in browser
	@open http://localhost:8082

open-airflow: ## Open Airflow UI in browser
	@open http://localhost:8086

open-minio: ## Open MinIO Console in browser
	@open http://localhost:9001

open-spark: ## Open Spark Master UI in browser
	@open http://localhost:8083

open-grafana: ## Open Grafana in browser
	@open http://localhost:3000

open-prometheus: ## Open Prometheus in browser
	@open http://localhost:9090

open-ml-docs: ## Open ML Server API docs in browser
	@open http://localhost:8000/docs

##@ Cleanup

clean: ## Remove containers, volumes (WARNING: data loss)
	@printf "$(RED)WARNING: This will delete all data. Continue? [y/N] $(RESET)" && read ans && [ $${ans:-N} = y ]
	docker compose down -v --remove-orphans
	@printf "$(GREEN)Cleanup complete.$(RESET)\n"

clean-logs: ## Clean Airflow logs
	@rm -rf airflow/logs/*
	@printf "$(GREEN)Logs cleaned.$(RESET)\n"

reset: ## Full reset - clean and rebuild from scratch
	$(MAKE) clean
	$(MAKE) setup

##@ Information

urls: ## Print all service URLs
	@printf "\n$(CYAN)========================================$(RESET)\n"
	@printf "$(CYAN)  Platform Service URLs$(RESET)\n"
	@printf "$(CYAN)========================================$(RESET)\n"
	@printf "  $(GREEN)Kafka UI:$(RESET)        http://localhost:8082\n"
	@printf "  $(GREEN)Airflow:$(RESET)         http://localhost:8086  (admin/admin123)\n"
	@printf "  $(GREEN)MinIO Console:$(RESET)   http://localhost:9001  (minioadmin/minioadmin123)\n"
	@printf "  $(GREEN)Spark Master:$(RESET)    http://localhost:8083\n"
	@printf "  $(GREEN)Grafana:$(RESET)         http://localhost:3000  (admin/grafana123)\n"
	@printf "  $(GREEN)Prometheus:$(RESET)      http://localhost:9090\n"
	@printf "  $(GREEN)ML API:$(RESET)          http://localhost:8000/docs\n"
	@printf "  $(GREEN)Schema Registry:$(RESET) http://localhost:8081\n"
	@printf "$(CYAN)========================================$(RESET)\n\n"

version: ## Show tool versions
	@printf "$(CYAN)Docker:$(RESET) " && docker --version
	@printf "$(CYAN)Docker Compose:$(RESET) " && docker compose version
	@printf "$(CYAN)Python:$(RESET) " && python3 --version

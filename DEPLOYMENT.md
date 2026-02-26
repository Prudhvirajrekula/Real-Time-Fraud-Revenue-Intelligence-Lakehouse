# Deployment Guide — Real-Time Fraud & Revenue Intelligence Platform

**Version**: 1.0
**Date**: 2026-02-25
**Environments**: Local (Docker Compose), AWS, Databricks

---

## Table of Contents

1. [Local Development](#local-development)
2. [AWS Production Architecture](#aws-production-architecture)
3. [AWS Service Mapping](#aws-service-mapping)
4. [Configuration Changes for AWS](#configuration-changes-for-aws)
5. [Databricks Alternative Mapping](#databricks-alternative-mapping)
6. [Production Checklist](#production-checklist)
7. [Cost Estimate (AWS)](#cost-estimate-aws)
8. [CI/CD Pipeline Design](#cicd-pipeline-design)
9. [Environment Promotion](#environment-promotion)
10. [Secrets Management](#secrets-management)

---

## Local Development

Local development uses Docker Compose with Colima as the container runtime on macOS Apple Silicon. Full setup instructions are documented in `README.md`. This section assumes you have already followed the Quick Start in `README.md` and have a running local cluster.

### Key Points for Local Development

- All services run in a single Docker Compose network (`fraud-platform-network`)
- Service discovery uses Docker DNS (e.g., `kafka:9092`, `postgres:5432`)
- MinIO simulates S3 with path-style addressing (`s3a://lakehouse/`)
- Spark standalone cluster replaces EMR/Databricks
- Airflow LocalExecutor replaces MWAA or self-hosted CeleryExecutor
- All credentials stored in `.env` (gitignored); `.env.example` provided as template

### Useful Local Commands

```bash
# Check all service health
docker compose ps

# Tail logs for a specific service
docker compose logs -f airflow-webserver

# Restart a single service without affecting others
docker compose restart ml-server

# Run a one-off Spark job manually
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /app/spark_jobs/bronze_ingestion.py --date 2026-02-25

# Connect to PostgreSQL
docker exec -it postgres psql -U postgres -d analytics

# Inspect Kafka topic
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic orders --from-beginning --max-messages 10

# Access MinIO CLI
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc ls local/lakehouse/
```

---

## AWS Production Architecture

The following diagram maps local Docker Compose services to their AWS-managed equivalents:

```
LOCAL (Docker Compose)              AWS PRODUCTION
====================================|======================================
Zookeeper + Kafka 7.5.3             | Amazon MSK (Kafka 3.5, m5.xlarge)
                                     | 3 brokers, Multi-AZ
                                     |
Schema Registry (Confluent)         | Confluent Cloud Schema Registry
                                     | (or self-hosted on EC2)
                                     |
MinIO (S3-compatible)               | Amazon S3 + AWS Glue Data Catalog
                                     | S3 Standard for hot data
                                     | S3 Intelligent-Tiering for archives
                                     |
Apache Spark (standalone)           | Amazon EMR on EC2 (m5.2xlarge)
                                     | OR Databricks on AWS
                                     | Auto-scaling EMR clusters
                                     |
Apache Airflow (LocalExecutor)      | Amazon MWAA (Managed Airflow 2.8.x)
                                     | OR self-hosted on EC2 with
                                     | CeleryExecutor + Redis
                                     |
PostgreSQL 15 (container)           | Amazon RDS PostgreSQL 15
                                     | db.r6g.xlarge, Multi-AZ
                                     | Automated backups, 7-day retention
                                     |
FastAPI ML Server (container)       | Amazon SageMaker Real-Time Endpoint
                                     | OR EC2 + ALB (Application Load Balancer)
                                     | Auto Scaling Group (min 2, max 10)
                                     |
Prometheus + Grafana                | Amazon CloudWatch + Grafana Cloud
                                     | OR Managed Grafana on AWS
                                     |
Great Expectations (standalone)     | AWS Glue Data Quality
                                     | OR self-hosted on EC2/Lambda
                                     |
dbt (container)                     | dbt Cloud OR dbt Core on MWAA
```

---

## AWS Service Mapping

### Amazon MSK (Kafka)

**Recommended configuration** for medium workload (~5M events/day):

```
Cluster type: Provisioned
Kafka version: 3.5.x (MSK-managed)
Broker type: kafka.m5.xlarge (4 vCPU, 16 GB RAM)
Number of brokers: 3 (one per Availability Zone)
Storage per broker: 1,000 GB (EBS gp3)
Replication factor: 3
min.insync.replicas: 2

Topic configuration:
  orders:     6 partitions, replication.factor=3, retention.ms=604800000 (7d)
  payments:   6 partitions, replication.factor=3, retention.ms=604800000 (7d)
  users:      6 partitions, replication.factor=3, retention.ms=1209600000 (14d)
  devices:    6 partitions, replication.factor=3, retention.ms=604800000 (7d)
  geo_events: 6 partitions, replication.factor=3, retention.ms=259200000 (3d)
  refunds:    6 partitions, replication.factor=3, retention.ms=2592000000 (30d)
```

**IAM policy for MSK client**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:WriteData"
      ],
      "Resource": [
        "arn:aws:kafka:us-east-1:ACCOUNT_ID:cluster/fraud-platform/*",
        "arn:aws:kafka:us-east-1:ACCOUNT_ID:topic/fraud-platform/*/*",
        "arn:aws:kafka:us-east-1:ACCOUNT_ID:group/fraud-platform/*"
      ]
    }
  ]
}
```

### Amazon S3 + AWS Glue Data Catalog

```
Bucket layout:
  s3://fraud-platform-lakehouse-prod/
    bronze/{topic}/event_date={YYYY-MM-DD}/
    silver/enriched_transactions/event_date={YYYY-MM-DD}/
    gold/{table}/event_date={YYYY-MM-DD}/
    checkpoints/bronze/{topic}/
    models/registry/{timestamp}/

S3 lifecycle rules:
  bronze/: Transition to S3-IA after 30 days, Glacier after 365 days
  silver/: Transition to S3-IA after 90 days
  gold/: No lifecycle (actively queried)
  models/: Retain all versions indefinitely (model governance)

Glue Data Catalog:
  Database: fraud_platform_bronze
  Database: fraud_platform_silver
  Database: fraud_platform_gold
  Tables registered via Glue Crawler (daily schedule) or Delta-native Glue integration
```

**S3 bucket policy** (KMS encryption):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::ACCOUNT_ID:role/fraud-platform-spark-role"},
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::fraud-platform-lakehouse-prod",
        "arn:aws:s3:::fraud-platform-lakehouse-prod/*"
      ]
    }
  ]
}
```

### Amazon EMR (Spark)

**Recommended EMR configuration**:

```
Release: emr-7.0.0 (includes Spark 3.5.x, Delta Lake 3.x)
Master: m5.xlarge (1 instance)
Core: m5.2xlarge (2-10 instances, auto-scaling)
Task: m5.xlarge (0-20 instances, spot, auto-scaling)

EMR auto-scaling policy:
  Scale out: Add 2 core nodes when YARN pending memory > 20 GB for 5 min
  Scale in:  Remove idle core nodes after 30 min of low utilization

Spark configuration (emr-spark-config.json):
  spark.sql.extensions: io.delta.sql.DeltaSparkSessionExtension
  spark.sql.catalog.spark_catalog: org.apache.spark.sql.delta.catalog.DeltaCatalog
  spark.sql.adaptive.enabled: true
  spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
  spark.hadoop.fs.s3a.aws.credentials.provider: com.amazonaws.auth.InstanceProfileCredentialsProvider
  spark.hadoop.fs.s3a.endpoint: s3.amazonaws.com
  spark.hadoop.fs.s3a.path.style.access: false  # AWS S3 uses virtual-hosted style
```

**Bootstrap script** (install Python dependencies on EMR nodes):
```bash
#!/bin/bash
# bootstrap-emr.sh
pip install \
  delta-spark==2.4.0 \
  great-expectations==0.18.9 \
  xgboost==2.0.0 \
  imbalanced-learn==0.11.0 \
  optuna==3.4.0
```

### Amazon RDS PostgreSQL

```
Engine: PostgreSQL 15.x
Instance: db.r6g.xlarge (4 vCPU, 32 GB RAM, Graviton2)
Storage: 500 GB gp3, autoscaling to 2 TB
Multi-AZ: Yes (synchronous standby in second AZ)
Read replicas: 1 (for dbt and analyst queries)
Backup: Automated daily snapshots, 7-day retention, PITR enabled
Encryption: AES-256 at rest, TLS in transit

Parameter group changes from default:
  shared_buffers: 8GB (25% of instance RAM)
  work_mem: 64MB
  max_connections: 200 (use PgBouncer in front)
  wal_level: replica
  max_wal_senders: 10
  checkpoint_timeout: 10min
```

### ML Serving: SageMaker vs EC2 + ALB

**Option A: SageMaker Real-Time Endpoint** (recommended for managed scaling):

```python
# deploy_to_sagemaker.py
import boto3
import sagemaker
from sagemaker.sklearn import SKLearnModel  # or custom container

sagemaker_session = sagemaker.Session()
model = sagemaker_session.create_model(
    name="fraud-xgboost-model",
    role="arn:aws:iam::ACCOUNT_ID:role/SageMakerExecutionRole",
    container_defs={
        "Image": "ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/fraud-ml-server:latest",
        "Environment": {
            "MODEL_PATH": "s3://fraud-platform-lakehouse-prod/models/registry/current/",
            "LOG_LEVEL": "INFO",
        }
    }
)

endpoint_config = sagemaker_session.create_endpoint_config(
    name="fraud-prediction-config",
    model_name="fraud-xgboost-model",
    initial_instance_count=2,
    instance_type="ml.m5.xlarge",
    variant_name="AllTraffic"
)
```

**Option B: EC2 + Application Load Balancer** (simpler, more control):

```
Launch Template: fraud-ml-server-lt
  AMI: Amazon Linux 2023
  Instance type: m5.xlarge (4 vCPU, 16 GB RAM)
  User data: docker pull + docker run ml-server container

Auto Scaling Group:
  Min: 2 instances (for HA)
  Max: 10 instances
  Target tracking: ALBRequestCountPerTarget < 500 req/min/instance

Application Load Balancer:
  Listener: HTTPS:443 (ACM certificate)
  Target group: ml-server instances on port 8000
  Health check: GET /health, 200 OK expected
  Stickiness: disabled (stateless inference)
```

### Monitoring: CloudWatch + Grafana Cloud

```
CloudWatch Metrics (custom namespace: FraudPlatform):
  - Kafka consumer lag (from MSK CloudWatch integration)
  - EMR cluster utilization
  - RDS CPU, connections, read/write IOPS
  - SageMaker invocation count, latency (p50/p95), error rate

Grafana Cloud:
  - Import CloudWatch as data source
  - Import existing dashboards (see monitoring/grafana/dashboards/)
  - Alert routing: PagerDuty for CRITICAL, Slack for WARNING
```

---

## Configuration Changes for AWS

### Environment Variables: Local vs AWS

| Variable | Local (.env) | AWS (Secrets Manager / Parameter Store) |
|----------|-------------|----------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | `b-1.fraud-platform.xxx.kafka.us-east-1.amazonaws.com:9092,...` |
| `MINIO_ENDPOINT` | `http://minio:9000` | Not used (S3 native) |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | IAM Role (no keys needed on EC2/EMR) |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | IAM Role (no keys needed on EC2/EMR) |
| `S3_BUCKET` | `lakehouse` | `fraud-platform-lakehouse-prod` |
| `SPARK_MASTER_URL` | `spark://spark-master:7077` | `yarn` (EMR) or `databricks://...` |
| `POSTGRES_HOST` | `postgres` | `fraud-platform.xxx.rds.amazonaws.com` |
| `POSTGRES_PORT` | `5432` | `5432` |
| `POSTGRES_DB` | `analytics` | `analytics` |
| `AIRFLOW_EXECUTOR` | `LocalExecutor` | `CeleryExecutor` |
| `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | Confluent Cloud Schema Registry URL |

### S3A Configuration for AWS S3

When switching from MinIO to real AWS S3, the following Spark configuration changes are required:

```python
# Local MinIO config
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
.config("spark.hadoop.fs.s3a.path.style.access", "true")  # MinIO requires path-style
.config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
.config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])

# AWS S3 config (IAM Role-based — no keys needed on EC2/EMR)
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
.config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider")
# path.style.access: false (AWS uses virtual-hosted-style by default)
```

### VPC and Security Group Design

```
VPC: fraud-platform-vpc (10.0.0.0/16)

Subnets:
  Private-A (10.0.1.0/24, us-east-1a): MSK, EMR, RDS
  Private-B (10.0.2.0/24, us-east-1b): MSK, EMR, RDS (Multi-AZ)
  Public-A  (10.0.3.0/24, us-east-1a): ALB, NAT Gateway

Security Groups:
  sg-kafka:
    Inbound: 9092 (PLAINTEXT) from sg-spark, sg-airflow, sg-data-generator
    Inbound: 9094 (TLS) from sg-spark, sg-airflow (production TLS)
    Outbound: All

  sg-spark:
    Inbound: 4040 (Spark UI) from sg-bastion
    Outbound: 9092 to sg-kafka, 443 to s3 (via S3 VPC Endpoint), 5432 to sg-rds

  sg-rds:
    Inbound: 5432 from sg-spark, sg-airflow, sg-analytics
    Outbound: None (RDS is egress-only via client connections)

  sg-alb:
    Inbound: 443 from 0.0.0.0/0 (public HTTPS)
    Outbound: 8000 to sg-ml-server

  sg-ml-server:
    Inbound: 8000 from sg-alb
    Outbound: 443 to s3 (model downloads), 5432 to sg-rds (optional feature queries)
```

### IAM Roles

```
fraud-platform-spark-role (attached to EMR EC2 instances):
  - AmazonS3FullAccess (restrict to fraud-platform-lakehouse-* bucket)
  - AmazonMSKReadWrite
  - CloudWatchAgentServerPolicy
  - SecretsManagerReadWrite (for Spark job credentials)

fraud-platform-airflow-role (attached to MWAA environment):
  - mwaa:* (MWAA self-reference for DAG execution)
  - s3:GetObject, s3:PutObject on dags/ prefix
  - AmazonMSKReadWrite (for connectivity checks)
  - AmazonRDSDataFullAccess

fraud-platform-ml-role (attached to SageMaker / EC2 ML instances):
  - s3:GetObject on models/registry/ prefix
  - cloudwatch:PutMetricData
  - secretsmanager:GetSecretValue
```

---

## Databricks Alternative Mapping

If the team chooses Databricks on AWS instead of EMR + open-source tooling:

| Component | Open-Source Stack | Databricks Equivalent |
|-----------|------------------|----------------------|
| Delta Lake | Delta Lake 2.4.0 (OSS) | Delta Lake (native in Databricks Runtime) |
| Spark | EMR + PySpark | Databricks Jobs (cluster-per-job or shared cluster) |
| MinIO/S3 | S3 + S3A connector | Unity Catalog external locations on S3 |
| Schema Registry | Confluent Schema Registry | Confluent Cloud or Schema Registry on EC2 |
| Airflow | Amazon MWAA | Databricks Workflows (replaces Airflow entirely) |
| dbt | dbt Core (CLI) | dbt Core on Databricks Jobs or dbt Cloud |
| ML training | XGBoost + Optuna (custom) | MLflow on Databricks (experiment tracking + registry) |
| ML serving | FastAPI (custom container) | MLflow Model Serving (Databricks serving endpoints) |
| Data quality | Great Expectations | Databricks Lakehouse Monitoring or Delta Live Tables expectations |

### Databricks Configuration Changes

```python
# Replace Spark standalone session config with Databricks connect
# (or submit as Databricks Job using the existing PySpark scripts unchanged)

# In Databricks Runtime, Delta Lake is pre-installed:
# No need for .config("spark.sql.extensions", ...)
# No need for S3A config (Unity Catalog handles S3 credentials)

# MLflow integration (replaces custom model registry):
import mlflow

mlflow.set_experiment("/fraud-detection/xgboost-training")
with mlflow.start_run():
    mlflow.log_params(best_params)
    mlflow.log_metric("roc_auc", auc)
    mlflow.xgboost.log_model(model, "fraud-model")
    mlflow.register_model(f"runs:/{mlflow.active_run().info.run_id}/fraud-model",
                          "FraudDetectionModel")
```

### Databricks Workflows (replacing Airflow DAGs)

```python
# databricks_workflow.py (Infrastructure as Code via Databricks SDK)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, SparkPythonTask, TaskDependency

w = WorkspaceClient()

job = w.jobs.create(
    name="fraud-platform-pipeline",
    tasks=[
        Task(
            task_key="bronze_ingestion",
            spark_python_task=SparkPythonTask(
                python_file="dbfs:/fraud-platform/spark_jobs/bronze_ingestion.py",
                parameters=["--date", "{{job.start_time.iso_date}}"]
            ),
            job_cluster_key="bronze-cluster",
        ),
        Task(
            task_key="silver_transformations",
            depends_on=[TaskDependency(task_key="bronze_ingestion")],
            spark_python_task=SparkPythonTask(
                python_file="dbfs:/fraud-platform/spark_jobs/silver_transform.py",
            ),
            job_cluster_key="silver-cluster",
        ),
        # ... gold, dbt, ml_training tasks
    ],
    schedule=CronSchedule(quartz_cron_expression="0 */2 * * * ?"),
)
```

---

## Production Checklist

### Security

- [ ] All inter-service communication uses TLS (Kafka SSL, HTTPS for APIs, RDS SSL)
- [ ] No credentials in source code or Docker images (verified by CI credential scan)
- [ ] All secrets stored in AWS Secrets Manager or HashiCorp Vault
- [ ] IAM roles follow least-privilege principle (no wildcard `*` actions on sensitive resources)
- [ ] S3 bucket versioning and MFA delete enabled on lakehouse bucket
- [ ] VPC Flow Logs enabled for network audit trail
- [ ] CloudTrail enabled for API audit trail
- [ ] RDS encryption at rest (AES-256) and in transit (TLS)
- [ ] SageMaker endpoint invocations logged to CloudWatch
- [ ] Security group rules reviewed: no `0.0.0.0/0` on internal services
- [ ] WAF enabled on ALB protecting ML server endpoint

### Networking

- [ ] All compute resources in private subnets (no public IP except bastion/ALB)
- [ ] S3 VPC Endpoint configured (traffic stays within AWS network)
- [ ] MSK VPC Endpoint or private connectivity configured
- [ ] NAT Gateway for outbound internet (software updates, pip packages)
- [ ] Bastion host or AWS Systems Manager Session Manager for admin access

### Scaling

- [ ] MSK Auto Scaling enabled (EBS storage scaling)
- [ ] EMR Auto Scaling policy tested with 3x expected peak load
- [ ] RDS read replica tested for dbt workload isolation
- [ ] SageMaker endpoint tested at p99 latency under load (Artillery or Locust)
- [ ] MWAA environment sizing validated (worker count, concurrency settings)

### Backup and Recovery

- [ ] RDS automated backups tested (restore to point-in-time verified)
- [ ] S3 bucket versioning verified (Delta transaction log recoverable)
- [ ] MWAA DAG S3 bucket backed up
- [ ] Model registry S3 prefix backed up (all model versions retained)
- [ ] Recovery Time Objective (RTO) documented and tested: < 1 hour
- [ ] Recovery Point Objective (RPO) documented: < 30 minutes (last Kafka batch)

### Monitoring

- [ ] CloudWatch alarms configured for all critical metrics
- [ ] PagerDuty integration configured for CRITICAL alerts
- [ ] Slack integration configured for WARNING alerts
- [ ] Grafana dashboards deployed and validated with realistic data
- [ ] Runbook documented for each CRITICAL alert

---

## Cost Estimate (AWS)

Estimated monthly cost for a medium production workload (~5M events/day, 2 regions for HA):

| Service | Configuration | Estimated Monthly Cost |
|---------|--------------|----------------------|
| Amazon MSK | 3x kafka.m5.xlarge, 1 TB EBS each | ~$450 |
| Amazon EMR | 1x m5.xlarge master + 2-4x m5.2xlarge core (avg 3 nodes) | ~$180 |
| Amazon RDS PostgreSQL | db.r6g.xlarge Multi-AZ + 500 GB storage | ~$320 |
| Amazon S3 | ~2 TB stored + request costs (Bronze/Silver/Gold) | ~$50 |
| Amazon MWAA | mw1.small environment, 2 workers | ~$150 |
| SageMaker Endpoint | 2x ml.m5.xlarge (min 2 for HA) | ~$140 |
| Amazon ECR | Container image storage (~20 GB) | ~$2 |
| NAT Gateway | 2 AZs, ~1 TB data processed | ~$100 |
| CloudWatch | Custom metrics, logs, alarms | ~$50 |
| Data Transfer | Cross-AZ + outbound (~500 GB) | ~$50 |
| Confluent Schema Registry | Basic tier (if using Confluent Cloud) | ~$0 (bundled with MSK free tier) or ~$50 (Confluent Cloud) |
| **TOTAL (estimated)** | | **~$1,492/month** |

**Notes**:
- EMR cost assumes spot instances for task nodes (~60% discount from on-demand), cutting EMR cost by ~30%
- MSK is the largest cost driver; using fewer partitions or smaller broker types reduces this
- RDS Multi-AZ doubles the single-AZ cost but is non-negotiable for production
- At 10x scale (50M events/day), MSK moves to m5.2xlarge (cost ~$900), EMR scales out (cost ~$400), RDS moves to db.r6g.2xlarge (cost ~$640) — total ~$2,200-2,500/month

**Cost optimization levers**:
1. EMR Spot instances for task nodes: -30-40% on compute
2. S3 Intelligent-Tiering for Bronze (older partitions move to lower-cost storage automatically)
3. Reserved Instances for RDS (1-year RI): ~40% discount
4. MSK Express broker type (simpler, lower cost for smaller workloads)

---

## CI/CD Pipeline Design

### GitHub Actions Workflow

```yaml
# .github/workflows/ci-cd.yml
name: Fraud Platform CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  # Stage 1: Lint and Static Analysis
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python 3.11
        uses: actions/setup-python@v4
        with: {python-version: "3.11"}
      - name: Install linting tools
        run: pip install ruff mypy black isort
      - name: Run ruff (fast linter)
        run: ruff check . --output-format=github
      - name: Check formatting (black)
        run: black --check .
      - name: Type check (mypy)
        run: mypy src/ --ignore-missing-imports
      - name: Credential audit (scan for hardcoded secrets)
        run: |
          pip install detect-secrets
          detect-secrets scan --all-files --baseline .secrets.baseline
          detect-secrets audit .secrets.baseline

  # Stage 2: Unit Tests
  unit-tests:
    runs-on: ubuntu-latest
    needs: lint
    steps:
      - uses: actions/checkout@v4
      - name: Set up Python 3.11
        uses: actions/setup-python@v4
        with: {python-version: "3.11"}
      - name: Install dependencies
        run: pip install -r requirements-test.txt
      - name: Run unit tests
        run: pytest tests/ -v --cov=src --cov-report=xml --cov-fail-under=80
      - name: Upload coverage
        uses: codecov/codecov-action@v3

  # Stage 3: dbt Tests (against staging database)
  dbt-tests:
    runs-on: ubuntu-latest
    needs: unit-tests
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_DB: analytics_test
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: ${{ secrets.POSTGRES_TEST_PASSWORD }}
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - name: Install dbt
        run: pip install dbt-postgres==1.7.0
      - name: Run dbt seed + run + test
        env:
          POSTGRES_HOST: localhost
          POSTGRES_PORT: 5432
          POSTGRES_DB: analytics_test
          POSTGRES_USER: postgres
          POSTGRES_PASSWORD: ${{ secrets.POSTGRES_TEST_PASSWORD }}
        run: |
          dbt deps --project-dir dbt --profiles-dir dbt
          dbt seed --project-dir dbt --profiles-dir dbt
          dbt run --project-dir dbt --profiles-dir dbt
          dbt test --project-dir dbt --profiles-dir dbt

  # Stage 4: Build Docker Images
  build:
    runs-on: ubuntu-latest
    needs: [unit-tests, dbt-tests]
    if: github.ref == 'refs/heads/main' || github.ref == 'refs/heads/develop'
    steps:
      - uses: actions/checkout@v4
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Login to Amazon ECR
        id: login-ecr
        uses: aws-actions/amazon-ecr-login@v2
      - name: Build and push Airflow image
        env:
          ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
          IMAGE_TAG: ${{ github.sha }}
        run: |
          docker build -t $ECR_REGISTRY/fraud-platform-airflow:$IMAGE_TAG \
            -f docker/Dockerfile.airflow .
          docker push $ECR_REGISTRY/fraud-platform-airflow:$IMAGE_TAG
          docker tag $ECR_REGISTRY/fraud-platform-airflow:$IMAGE_TAG \
            $ECR_REGISTRY/fraud-platform-airflow:latest
          docker push $ECR_REGISTRY/fraud-platform-airflow:latest
      - name: Build and push ML Server image
        env:
          ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
          IMAGE_TAG: ${{ github.sha }}
        run: |
          docker build -t $ECR_REGISTRY/fraud-platform-ml-server:$IMAGE_TAG \
            -f docker/Dockerfile.mlserver .
          docker push $ECR_REGISTRY/fraud-platform-ml-server:$IMAGE_TAG

  # Stage 5: Deploy to Staging
  deploy-staging:
    runs-on: ubuntu-latest
    needs: build
    if: github.ref == 'refs/heads/develop'
    environment: staging
    steps:
      - name: Deploy MWAA DAGs to staging
        run: |
          aws s3 sync airflow/dags/ \
            s3://fraud-platform-mwaa-staging/dags/ \
            --delete \
            --region us-east-1
      - name: Update MWAA environment (trigger DAG refresh)
        run: |
          aws mwaa update-environment \
            --name fraud-platform-staging \
            --region us-east-1
      - name: Update ML Server (blue/green deployment)
        run: |
          aws ecs update-service \
            --cluster fraud-platform-staging \
            --service ml-server \
            --force-new-deployment \
            --region us-east-1
      - name: Run smoke tests against staging
        run: bash scripts/run_smoke_test.sh --env staging

  # Stage 6: Deploy to Production (manual approval required)
  deploy-prod:
    runs-on: ubuntu-latest
    needs: deploy-staging
    if: github.ref == 'refs/heads/main'
    environment:
      name: production
      url: https://fraud-platform.example.com
    steps:
      - name: Deploy to production
        run: |
          aws s3 sync airflow/dags/ \
            s3://fraud-platform-mwaa-prod/dags/ \
            --delete
          aws ecs update-service \
            --cluster fraud-platform-prod \
            --service ml-server \
            --force-new-deployment
      - name: Verify production health
        run: bash scripts/run_smoke_test.sh --env prod
      - name: Notify deployment success
        uses: slackapi/slack-github-action@v1
        with:
          channel-id: "#platform-deployments"
          slack-message: "Fraud Platform deployed to production: ${{ github.sha }}"
```

---

## Environment Promotion

### Branch and Environment Strategy

```
Feature Branch (feature/xyz)
    |  PR → code review → unit tests pass
    v
develop branch
    |  Auto-deploy to STAGING on push
    |  Smoke tests run against staging
    v
main branch (requires PR approval + all CI checks green)
    |  Manual approval gate in GitHub Actions "production" environment
    v
PRODUCTION
```

### Environment Differences

| Configuration | Development (local) | Staging (AWS) | Production (AWS) |
|---------------|---------------------|---------------|-----------------|
| Kafka brokers | 1 (single container) | 3 (MSK, m5.large) | 3 (MSK, m5.xlarge) |
| Spark workers | 2 (containers) | 2 (EMR m5.xlarge) | 4-8 (EMR m5.2xlarge, auto-scaling) |
| RDS instance | N/A (container) | db.t3.medium (single AZ) | db.r6g.xlarge (Multi-AZ) |
| Data volume | Synthetic, ~50 ev/sec | Synthetic, ~500 ev/sec | Production data |
| Airflow executor | LocalExecutor | CeleryExecutor (2 workers) | CeleryExecutor (10 workers) |
| Monitoring | Local Prometheus/Grafana | CloudWatch + Grafana Cloud | CloudWatch + Grafana Cloud + PagerDuty |
| SSL/TLS | No | Yes (self-signed or ACM) | Yes (ACM, externally validated) |
| Data retention | 7 days (disk space) | 30 days | 1 year (with lifecycle tiering) |

### Promotion Artifacts

Artifacts that must be explicitly promoted (not redeployed) from staging to production:

1. **ML Models**: Trained in staging on staging data; production models are trained in production (never promote a model trained on staging data to production)
2. **dbt Compiled Artifacts**: `dbt compile` output verified in staging before running `dbt run` in production
3. **Great Expectations Checkpoints**: Expectation suites tested against staging data profile before applying to production
4. **Airflow DAG Variables**: Staging uses staging S3 paths, staging database; production uses production values (managed via MWAA environment variables, not code)

---

## Secrets Management

### Local Development

Secrets are stored in `.env` (gitignored). `.env.example` provides a template:

```bash
# .env.example (commit this file)
# Copy to .env and fill in values — never commit .env

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081

# MinIO / S3
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=your_access_key_here
MINIO_SECRET_KEY=your_secret_key_here
S3_BUCKET=lakehouse

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=analytics
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password_here

# Airflow
AIRFLOW_FERNET_KEY=your_fernet_key_here
AIRFLOW_ADMIN_PASSWORD=your_admin_password_here

# Grafana
GRAFANA_PASSWORD=your_grafana_password_here

# ML Server
ML_MODEL_PATH=/app/models/registry/current
```

### AWS Production: AWS Secrets Manager

In production, all secrets are stored in AWS Secrets Manager and injected into services at runtime:

```python
# utils/secrets.py — used by all services in production
import boto3
import json
import os

def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    """Retrieve a secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

def get_db_credentials() -> dict:
    """Get PostgreSQL credentials from Secrets Manager."""
    env = os.environ.get("ENVIRONMENT", "prod")
    return get_secret(f"fraud-platform/{env}/postgres")

def get_kafka_credentials() -> dict:
    """Get Kafka credentials from Secrets Manager."""
    env = os.environ.get("ENVIRONMENT", "prod")
    return get_secret(f"fraud-platform/{env}/kafka")
```

**Secret naming convention**:
```
fraud-platform/prod/postgres      -> {host, port, dbname, username, password}
fraud-platform/prod/kafka         -> {bootstrap_servers, sasl_username, sasl_password}
fraud-platform/prod/minio         -> {endpoint, access_key, secret_key}
fraud-platform/prod/airflow       -> {fernet_key, admin_password}
fraud-platform/prod/grafana       -> {admin_password}
```

### HashiCorp Vault Alternative

For teams running multi-cloud or on-premises infrastructure, HashiCorp Vault provides a cloud-agnostic secrets management solution:

```bash
# Vault secret path convention
vault kv put secret/fraud-platform/prod/postgres \
  host="fraud-platform.xxx.rds.amazonaws.com" \
  port="5432" \
  dbname="analytics" \
  username="fraud_app" \
  password="$(openssl rand -base64 32)"

# Dynamic database credentials (Vault PostgreSQL secrets engine)
vault secrets enable database
vault write database/config/fraud-platform-postgres \
  plugin_name=postgresql-database-plugin \
  connection_url="postgresql://{{username}}:{{password}}@fraud-platform.xxx.rds.amazonaws.com:5432/analytics" \
  allowed_roles="fraud-app-role" \
  username="vault_admin" \
  password="$(cat /run/secrets/vault_postgres_admin)"
```

Dynamic credentials (short-lived, auto-rotated passwords generated by Vault) are the gold standard for production database access — they eliminate the risk of credential exposure from long-lived passwords.

### Rotation Policy

| Secret | Rotation Frequency | Method |
|--------|-------------------|--------|
| PostgreSQL passwords | 30 days | AWS Secrets Manager auto-rotation with Lambda |
| Airflow Fernet key | 90 days | Manual rotation (requires DAG variable re-encryption) |
| Kafka SASL credentials | 90 days | MSK rotation via Secrets Manager |
| ML model signing key | On model version bump | CI/CD pipeline |
| Grafana admin password | 90 days | Manual |

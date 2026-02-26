# System Design — Real-Time Fraud & Revenue Intelligence Platform

**Version**: 1.0
**Date**: 2026-02-25
**Author**: Platform Engineering
**Status**: Production-validated (local), AWS-deployable

---

## Table of Contents

1. [Overview and Design Goals](#overview-and-design-goals)
2. [Full Architecture Deep Dive](#full-architecture-deep-dive)
3. [Data Flow Per Layer](#data-flow-per-layer)
4. [Kafka Topic Design](#kafka-topic-design)
5. [Delta Lake Design](#delta-lake-design)
6. [Spark Structured Streaming Design](#spark-structured-streaming-design)
7. [PostgreSQL Warehouse Design](#postgresql-warehouse-design)
8. [Airflow DAG Design](#airflow-dag-design)
9. [ML Pipeline Design](#ml-pipeline-design)
10. [Failure Handling](#failure-handling)
11. [Backpressure Mechanisms](#backpressure-mechanisms)
12. [Partitioning Strategy](#partitioning-strategy)
13. [Scalability Bottlenecks](#scalability-bottlenecks)
14. [Monitoring and Alerting](#monitoring-and-alerting)
15. [Known Limitations and Future Improvements](#known-limitations-and-future-improvements)

---

## Overview and Design Goals

This platform ingests, processes, and analyzes e-commerce transaction events in near real-time to detect fraud and compute revenue intelligence metrics. The system is architected as a lakehouse: a streaming-first ingestion layer backed by object storage with ACID table semantics (Delta Lake), feeding a relational analytics warehouse (PostgreSQL) and a real-time ML inference service (FastAPI + XGBoost).

### Design Goals

| Goal | Requirement | Implementation |
|------|-------------|---------------|
| Correctness | No duplicate records in Silver or Gold layers | Delta Lake MERGE (upsert) with idempotency keys |
| Recoverability | Any layer can be rebuilt from upstream without re-reading Kafka | Bronze is the source of truth; Silver/Gold derived by deterministic transforms |
| Observability | Every layer emits metrics consumable by Prometheus | JMX exporters, Spark metrics sink, StatsD, postgres_exporter |
| Security | No hardcoded credentials anywhere | All secrets via environment variables; credential audit in CI |
| Portability | Local-to-cloud migration requires only configuration changes | Spark master URL, S3 endpoint, and JDBC URL are environment variables |
| Testability | Platform health verifiable with a single script | 19 smoke tests, 39 unit tests, 34 dbt tests |
| ML governance | Model only promoted if quality gates pass | ROC-AUC gate, feature count validation, row count floor |

### Non-Goals

- Sub-second (streaming) ML inference at ingestion time — fraud scores are computed as a batch score against the Silver layer, not inline with Kafka messages. Real-time scoring via the FastAPI endpoint is for API consumers, not automatic transaction blocking.
- Multi-region replication — this is a single-region (or single-machine) design.
- GDPR right-to-erasure — Bronze layer is append-only and does not support individual record deletion. Delta Lake `DELETE` could implement this at Silver, but it is out of scope.

---

## Full Architecture Deep Dive

### Component Responsibilities

#### Data Generator

- **Technology**: Python 3.11, custom producer classes per topic
- **Responsibility**: Emits synthetic but realistic transactional events at ~50 events/sec across 6 Kafka topics. Each generator maintains internal state to produce coherent sequences (e.g., a payment always references an existing order_id).
- **Fraud injection**: ~3% of events are flagged as fraudulent with correlated feature patterns (high velocity, geo anomaly, new device) to produce a learnable signal for the ML model.

#### Kafka + Schema Registry

- **Technology**: Confluent Kafka 7.5.3, Confluent Schema Registry (bundled), Zookeeper
- **Responsibility**: Durable, ordered, replayable event log. Schema Registry enforces Avro schemas at the wire level — a producer attempting to publish a schema-incompatible message is rejected before the message reaches any consumer.
- **Consumer groups**: `data-generator-producers` (write), `spark-bronze-consumer` (Bronze Spark jobs), `data-quality-consumer` (Great Expectations standalone runner)

#### Spark Structured Streaming (Bronze Ingestion)

- **Technology**: Apache Spark 3.5.5, PySpark, Delta Lake 2.4.0, hadoop-aws (S3A connector)
- **Responsibility**: Consumes from all 6 Kafka topics, applies minimal schema coercion, writes to Bronze Delta tables partitioned by `event_date`. Maintains Kafka offset checkpoints in MinIO to resume after restart.
- **Trigger**: `Trigger.ProcessingTime("5 minutes")` for Bronze — balances latency with file size efficiency.

#### MinIO (Object Storage)

- **Technology**: MinIO latest (S3-compatible)
- **Responsibility**: Stores all Delta Lake data (Bronze, Silver, Gold) and Spark checkpoint state. Accessed via the S3A connector with path-style addressing (`s3a://lakehouse/`).
- **Bucket layout**: `lakehouse/bronze/`, `lakehouse/silver/`, `lakehouse/gold/`, `lakehouse/checkpoints/`

#### Spark Batch Jobs (Silver and Gold Transformations)

- **Technology**: PySpark, Delta Lake MERGE, Great Expectations (Silver validation)
- **Responsibility**: Silver reads from Bronze, joins across tables, computes ML features, and upserts into Silver Delta. Gold reads from Silver, computes aggregations, and upserts into Gold Delta + loads via JDBC to PostgreSQL.
- **Trigger**: Invoked by Airflow DAG tasks; not streaming — these are batch micro-jobs running on a schedule.

#### Airflow

- **Technology**: Apache Airflow 2.8.1, LocalExecutor, custom Dockerfile
- **Responsibility**: Orchestrates the Bronze → Silver → Gold → dbt → ML training pipeline. Enforces cross-DAG dependencies via ExternalTaskSensor. Manages retries, SLA alerts, and DAG-level backfill.

#### PostgreSQL

- **Technology**: PostgreSQL 15
- **Responsibility**: Analytics warehouse. Receives Gold-layer aggregations via Spark JDBC. Serves as the dbt source database. Supports ad hoc SQL analysis by data analysts.

#### dbt

- **Technology**: dbt-postgres 1.7
- **Responsibility**: Transforms Gold-layer data loaded into PostgreSQL into a star schema (fact + dimension tables) for BI consumption. All transformations are declarative SQL with version-controlled tests.

#### Great Expectations

- **Technology**: Great Expectations 0.18.9
- **Responsibility**: Standalone `DataQualityRunner` validates Silver-layer Delta tables after each Silver transformation. Runs expectation suites covering row counts, null rates, value ranges, and referential integrity checks.

#### ML Server (FastAPI + XGBoost)

- **Technology**: FastAPI 0.104, XGBoost 2.0, imbalanced-learn (SMOTE), Optuna
- **Responsibility**: Serves the fraud detection model. Exposes `/predict` (single transaction), `/predict/batch` (batch of transactions), `/model/reload` (hot-swap model without downtime). Model versioning via timestamped directories in `/app/models/registry/`.

#### Prometheus + Grafana

- **Technology**: Prometheus latest, Grafana latest
- **Responsibility**: Prometheus scrapes JMX exporters (Kafka), Spark metrics sink, Airflow StatsD exporter, postgres_exporter, and the ML server `/metrics` endpoint. Grafana provides dashboards and alert routing.

---

## Data Flow Per Layer

### Layer 1: Bronze — Raw Event Capture

```
Kafka Topic (orders)
        |
        | readStream (Spark Structured Streaming)
        | - subscribe: ["orders", "payments", "users", "devices", "geo_events", "refunds"]
        | - startingOffsets: "latest" (first run), checkpoint (subsequent)
        | - maxOffsetsPerTrigger: 10000 per partition
        v
Spark Micro-Batch Processing
        |
        | - Parse Avro payload via Schema Registry deserializer
        | - Add metadata columns: kafka_offset, kafka_partition, ingested_at, event_date
        | - Minimal schema: preserve raw_payload as STRING for auditability
        v
Delta Lake APPEND
        |
        | - Trigger: ProcessingTime("5 minutes")
        | - Output mode: append
        | - Checkpoint: s3a://lakehouse/checkpoints/bronze/{topic}/
        | - Partition: event_date
        v
s3a://lakehouse/bronze/{topic}/event_date={YYYY-MM-DD}/part-*.snappy.parquet
```

**Guarantees**: Exactly-once delivery via Kafka offset checkpointing in Delta Lake transaction log. If the Spark job restarts mid-batch, it resumes from the last committed checkpoint offset.

### Layer 2: Silver — Enrichment and Feature Engineering

```
Bronze Delta Tables (all 6 topics)
        |
        | Airflow Task: silver_transform (PySpark batch job)
        | - Filter: event_date = {{ ds }} (date parameter from Airflow)
        | - Read via spark.read.format("delta") with partition filter
        v
Cross-Topic Join
        |
        | orders LEFT JOIN payments  ON order_id
        | orders LEFT JOIN devices   ON (user_id, session_id)
        | orders LEFT JOIN geo_events ON (user_id, timestamp within 5min window)
        | orders LEFT JOIN users     ON user_id
        | orders LEFT JOIN refunds   ON order_id (for is_fraud label)
        v
Feature Engineering (25 columns)
        |
        | - Window functions: velocity_1h, velocity_24h (partitioned by user_id)
        | - UDFs: amount_zscore, device_risk_score, geo_anomaly_flag
        | - Lookups: payment_method_risk (broadcast join), merchant_fraud_rate
        v
Great Expectations Validation
        |
        | - Expectation suite: silver_enriched_expectations.json
        | - Checks: null rates, value ranges, feature column completeness
        | - On failure: raise exception -> Airflow task FAILS -> blocks Gold
        v
Delta Lake MERGE (upsert)
        |
        | MERGE INTO silver.enriched_transactions AS target
        | USING staged_records AS source
        | ON target.transaction_id = source.transaction_id
        |    AND target.event_date = source.event_date
        | WHEN MATCHED THEN UPDATE SET *
        | WHEN NOT MATCHED THEN INSERT *
        v
s3a://lakehouse/silver/enriched_transactions/event_date={YYYY-MM-DD}/
```

**Guarantees**: Idempotent MERGE means reruns (due to Airflow retries) produce no duplicates. Great Expectations acts as a data contract: Silver data only reaches Gold if it meets the expectation suite.

### Layer 3: Gold — Aggregations and Business Metrics

```
Silver Delta Table (enriched_transactions)
        |
        | Airflow Task: gold_aggregate (PySpark batch job)
        | - Filter: event_date = {{ ds }}
        v
Aggregation Queries
        |
        | revenue_hourly:
        |   GROUP BY merchant_id, date_trunc('hour', created_at)
        |   SUM(amount), COUNT(*), COUNT(CASE WHEN is_fraud THEN 1 END)
        |
        | fraud_metrics_daily:
        |   GROUP BY merchant_id, region, event_date
        |   fraud_rate, chargeback_rate, avg_transaction_amt, p95_transaction_amt
        |
        | merchant_summary:
        |   Rolling 30-day aggregates per merchant_id
        |
        | cohort_analysis:
        |   User cohort (registration month) x conversion metrics
        v
Delta MERGE to Gold + JDBC load to PostgreSQL
        |
        | 1. MERGE into Gold Delta tables (for time-travel and reprocessing)
        | 2. spark.write.jdbc() to PostgreSQL gold schema (truncate-insert per partition)
        v
s3a://lakehouse/gold/{table}/event_date={YYYY-MM-DD}/
PostgreSQL: gold.revenue_hourly, gold.fraud_metrics_daily, gold.merchant_summary
```

---

## Kafka Topic Design

### Topic Configuration

| Topic | Partitions | Replication | Retention | Schema | Consumer Groups |
|-------|-----------|-------------|-----------|--------|----------------|
| orders | 3 | 1 (local) | 7 days | Avro (Schema Registry) | spark-bronze-consumer, data-quality-consumer |
| payments | 3 | 1 (local) | 7 days | Avro | spark-bronze-consumer |
| users | 3 | 1 (local) | 14 days | Avro | spark-bronze-consumer |
| devices | 3 | 1 (local) | 7 days | Avro | spark-bronze-consumer |
| geo_events | 3 | 1 (local) | 3 days | Avro | spark-bronze-consumer |
| refunds | 3 | 1 (local) | 30 days | Avro | spark-bronze-consumer |

**Partition key strategy**: All topics use `user_id` as the message key. This ensures all events for a given user land on the same partition, preserving per-user ordering — important for velocity feature computation in Silver.

### Partition Count Rationale

3 partitions per topic is sufficient for the current ~50 events/sec throughput (Kafka can sustain thousands of messages/sec per partition). The number was chosen to allow natural parallelism (3 Spark tasks per topic, matchable to Spark worker cores) while keeping the partition count manageable. At 10x scale, 12 partitions (matching 3 Spark cores per worker × 2 workers × 2 for headroom) would be appropriate.

### Schema Evolution Policy

The Schema Registry is configured with **`BACKWARD`** compatibility mode:
- New fields may be added with a default value (consumers can ignore unknown fields)
- Existing fields may not be renamed or removed without a major version bump
- Type changes require a new subject version with full compatibility review

Schema evolution is tracked via the Schema Registry REST API. Airflow's `data_quality` DAG includes a task that checks for schema version drift and alerts if any topic has an unexpected schema version bump.

### Consumer Group Isolation

- `spark-bronze-consumer`: Reads from all 6 topics. Offset management delegated to Delta Lake checkpoint (Spark ignores Kafka's stored offsets and manages its own).
- `data-quality-consumer`: Reads samples from Silver Delta (not Kafka directly) for Great Expectations validation. This group exists for future direct-from-Kafka quality checks.
- Consumer group isolation means Spark Bronze ingestion and Great Expectations validation never interfere with each other's offsets.

---

## Delta Lake Design

### ACID Guarantees

Delta Lake wraps Parquet files with a JSON transaction log (`_delta_log/`) that records every write operation. This provides:

- **Atomicity**: A write either commits fully (all files referenced in the log) or not at all (partial file writes are invisible).
- **Consistency**: Schema enforcement prevents type mismatches from corrupting existing data.
- **Isolation**: Concurrent readers see a consistent snapshot; writers use optimistic concurrency with conflict detection.
- **Durability**: The transaction log is written to MinIO before the operation is considered committed.

### MERGE Strategy for Upserts

Silver and Gold layers use MERGE (not INSERT OVERWRITE) to handle reprocessing:

```sql
-- Silver upsert pattern
MERGE INTO delta.`s3a://lakehouse/silver/enriched_transactions` AS target
USING (SELECT * FROM staged_records WHERE event_date = '{run_date}') AS source
ON target.transaction_id = source.transaction_id
   AND target.event_date = source.event_date
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
```

**Why MERGE over INSERT OVERWRITE**: INSERT OVERWRITE deletes and rewrites the entire partition, which is dangerous if the job fails mid-write (partition is temporarily empty). MERGE is atomic and can be retried safely.

**Idempotency key**: `(transaction_id, event_date)` — globally unique per business event and per processing day.

### Z-ordering

Gold-layer tables most frequently queried with `WHERE merchant_id = 'X' AND event_date BETWEEN '...' AND '...'` are Z-ordered on `(merchant_id, event_date)`:

```python
DeltaTable.forPath(spark, gold_path).optimize().executeZOrderBy("merchant_id", "event_date")
```

Z-ordering interleaves the sort keys in a space-filling curve, ensuring that files containing a given `merchant_id` are co-located on MinIO. Delta's data skipping statistics then allow Spark to skip 80-95% of files for merchant-scoped queries.

**OPTIMIZE schedule**: Currently manual (run after large batch writes). Production plan: weekly Airflow task running `OPTIMIZE` + `VACUUM RETAIN 168 HOURS`.

### Checkpointing

Spark Structured Streaming (Bronze layer) uses Delta Lake checkpointing:
- Checkpoint location: `s3a://lakehouse/checkpoints/bronze/{topic}/`
- Checkpoint stores Kafka offsets and Delta Lake transaction ID of last successful write
- On Spark job restart: reads checkpoint, determines last committed Kafka offset, resumes from next offset
- This provides **exactly-once** semantics: each Kafka message is written to Bronze exactly once even across restarts

### Time Travel

Delta Lake's transaction log enables time-travel queries:

```python
# Read Bronze as it was 24 hours ago (for debugging)
spark.read.format("delta") \
  .option("timestampAsOf", "2026-02-24 00:00:00") \
  .load("s3a://lakehouse/bronze/orders/")

# Rebuild Silver from Bronze as of yesterday (for reprocessing)
spark.read.format("delta") \
  .option("versionAsOf", 42) \
  .load("s3a://lakehouse/silver/enriched_transactions/")
```

Transaction log is retained for 30 days (configurable via `delta.logRetentionDuration`). Underlying Parquet files are retained until `VACUUM` is run with a retention threshold.

---

## Spark Structured Streaming Design

### Job Configuration

```python
spark = SparkSession.builder \
    .appName("BronzeIngestion") \
    .master(os.environ["SPARK_MASTER_URL"]) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"]) \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"]) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()
```

### Trigger Intervals

| Layer | Trigger Type | Interval | Rationale |
|-------|-------------|----------|-----------|
| Bronze | ProcessingTime | 5 minutes | Balance latency vs. small file accumulation |
| Silver | Airflow-triggered batch | 30 minutes | Downstream depends on full partition completion |
| Gold | Airflow-triggered batch | 2 hours | Aggregations are inherently near-real-time; 2h is sufficient |

### Watermarking for Late Data

Bronze streaming jobs apply a 10-minute watermark on the `event_timestamp` field parsed from the Kafka message:

```python
df.withWatermark("event_timestamp", "10 minutes")
```

This means events arriving more than 10 minutes late are still included in Bronze (they are appended with the correct `event_date` derived from `event_timestamp`, not from `ingested_at`). Events arriving more than 10 minutes late do not contribute to aggregation windows in Silver — they are captured in Bronze but flagged with `late_arrival=true` for separate analysis.

### Output Modes

| Layer | Output Mode | Rationale |
|-------|------------|-----------|
| Bronze | append | Immutable raw log; existing records never modified |
| Silver | complete (via MERGE) | Upsert semantics; not a Spark output mode but a Delta operation |
| Gold | complete (via MERGE) | Aggregations replace previous values for the same grain |

### Backpressure via maxOffsetsPerTrigger

```python
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"]) \
    .option("subscribe", "orders,payments,users,devices,geo_events,refunds") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()
```

`maxOffsetsPerTrigger=10000` per partition means each micro-batch processes at most 10,000 messages per partition (60,000 total across 6 topics × 3 partitions per topic... wait — with 3 partitions per topic, total cap per batch is 10,000 × 3 × 6 = 180,000 messages). This prevents memory pressure during lag catch-up (e.g., after a Spark restart with hours of accumulated messages).

---

## PostgreSQL Warehouse Design

### Schema Layout

```
PostgreSQL 15 / database: analytics

Schemas:
  staging/     -- Raw Gold-layer data loaded by Spark JDBC; untransformed
  intermediate/-- dbt intermediate models; business logic before final shape
  gold/        -- dbt-renamed Gold aggregations; used by marts
  silver/      -- Selected Silver columns loaded for dbt enrichment
  marts/       -- Final star schema; consumed by BI tools and analysts
```

### Star Schema (marts schema)

```
marts.fact_transactions
  transaction_id      VARCHAR(50)   PK
  user_key            INT           FK -> dim_users
  merchant_key        INT           FK -> dim_merchants
  date_key            INT           FK -> dim_date
  amount              DECIMAL(15,4)
  currency            CHAR(3)
  is_fraud            BOOLEAN
  fraud_score         DECIMAL(5,4)  -- from ML model
  payment_method      VARCHAR(50)
  order_status        VARCHAR(30)

marts.dim_users
  user_key            INT           PK (surrogate)
  user_id             VARCHAR(50)   NK (natural key)
  registration_date   DATE
  country             VARCHAR(50)
  account_age_days    INT
  chargeback_count    INT
  valid_from          TIMESTAMP     -- SCD Type 2
  valid_to            TIMESTAMP
  is_current          BOOLEAN

marts.dim_merchants
  merchant_key        INT           PK
  merchant_id         VARCHAR(50)   NK
  merchant_name       VARCHAR(255)
  category            VARCHAR(100)
  country             VARCHAR(50)
  fraud_rate_30d      DECIMAL(5,4)

marts.dim_date
  date_key            INT           PK
  full_date           DATE
  year                INT
  quarter             INT
  month               INT
  week                INT
  day_of_week         INT
  is_weekend          BOOLEAN
  is_holiday          BOOLEAN
```

### Indexing Strategy

```sql
-- Fact table: partition by event_date, index on common filter columns
CREATE INDEX idx_fact_txn_date ON marts.fact_transactions(date_key);
CREATE INDEX idx_fact_txn_merchant ON marts.fact_transactions(merchant_key);
CREATE INDEX idx_fact_txn_fraud ON marts.fact_transactions(is_fraud) WHERE is_fraud = true;

-- Partial index for fraud-only queries (common in fraud analytics)
CREATE INDEX idx_fact_txn_fraud_partial
  ON marts.fact_transactions(merchant_key, date_key)
  WHERE is_fraud = true;
```

---

## Airflow DAG Design

### DAG Inventory

| DAG ID | Schedule | Owner | Dependency | Description |
|--------|----------|-------|------------|-------------|
| bronze_ingestion | `*/30 * * * *` | platform-engineering | None | Triggers Spark Bronze jobs for all 6 topics |
| silver_transformations | `*/30 * * * *` | platform-engineering | ExternalTaskSensor(bronze_ingestion) | Silver enrichment + Great Expectations validation |
| gold_aggregations | `0 */2 * * *` | platform-engineering | ExternalTaskSensor(silver_transformations) | Gold aggregations + JDBC load to PostgreSQL |
| data_quality | `0 * * * *` | data-quality | None (independent) | Great Expectations standalone runner against Silver |
| dbt_transformations | `0 */2 * * *` | analytics-engineering | ExternalTaskSensor(gold_aggregations) | dbt run + dbt test (8 models, 34 tests) |
| ml_fraud_training | `0 2 * * *` | ml-platform | ExternalTaskSensor(silver_transformations) | XGBoost training, quality gates, model promotion |

### DAG Dependency Chain

```
bronze_ingestion (every 30min)
        |
        | ExternalTaskSensor
        v
silver_transformations (every 30min)
        |
        +------ ExternalTaskSensor ------+
        |                                |
        v                                v
gold_aggregations (every 2h)     ml_fraud_training (daily 2am)
        |
        | ExternalTaskSensor
        v
dbt_transformations (every 2h)
```

`data_quality` DAG runs independently on an hourly schedule — it is not in the dependency chain because quality failures should not block business reporting. Instead, quality failures trigger Prometheus alerts.

### Task Design within bronze_ingestion

```
bronze_ingestion DAG
  |
  +-- check_kafka_connectivity (PythonOperator)
  |     - Verifies Kafka broker is reachable
  |     - Fails fast before submitting Spark job
  |
  +-- submit_bronze_spark_job (SparkSubmitOperator)
  |     - Submits PySpark job to standalone cluster
  |     - Passes {{ ds }} as --date argument for partition targeting
  |     - retries=3, retry_delay=timedelta(minutes=5)
  |
  +-- verify_bronze_delta_partition (PythonOperator)
        - Reads Delta transaction log to confirm partition written
        - Counts rows; fails if count = 0
        - This is the sensor that silver_transformations waits on
```

### ExternalTaskSensor Configuration

```python
wait_for_bronze = ExternalTaskSensor(
    task_id="wait_for_bronze_completion",
    external_dag_id="bronze_ingestion",
    external_task_id="verify_bronze_delta_partition",
    execution_delta=timedelta(minutes=0),
    timeout=3600,        # Wait up to 1 hour before failing
    poke_interval=60,    # Check every 60 seconds
    mode="reschedule",   # Release worker slot while waiting
    dag=dag,
)
```

`mode="reschedule"` is critical: it releases the Airflow worker slot while waiting, avoiding deadlock when LocalExecutor has limited parallelism.

### Retry Policy

All Spark-submitting tasks use:
- `retries=3`
- `retry_delay=timedelta(minutes=5)`
- `retry_exponential_backoff=False` (fixed delay for predictable scheduling)

Delta MERGE idempotency means retries are safe — a task that failed after writing some files but before committing will simply re-process and MERGE the same data again with no duplicates.

---

## ML Pipeline Design

### Feature Store Architecture

The Silver Delta table functions as the feature store. This is a deliberate design choice: rather than maintaining a separate feature computation pipeline (and the associated training/serving skew risk), Silver columns computed during the ETL pass are reused directly for model training.

```
Silver enriched_transactions Delta table
  |
  | (partitioned by event_date)
  |
  +-- ETL consumers (analytics)
  |   SELECT merchant_id, SUM(amount), COUNT(*) ...
  |
  +-- ML consumers (training)
      SELECT velocity_1h, chargeback_count, ..., is_fraud
      FROM silver.enriched_transactions
      WHERE event_date BETWEEN '...' AND '...'
```

**Training/serving consistency**: The FastAPI `/predict` endpoint receives a JSON payload and applies the same 25 feature transformations that are pre-computed in Silver. There is no separate feature computation at inference time — the caller is expected to provide pre-computed features (or a lightweight client-side feature computation step). This eliminates training/serving skew.

### SMOTE Application

```python
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split

X = df[FEATURE_COLUMNS]
y = df["is_fraud"]

# Split BEFORE applying SMOTE to avoid data leakage
X_train, X_val, y_train, y_val = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# Apply SMOTE only to training set
smote = SMOTE(sampling_strategy=1.0, random_state=42, k_neighbors=5)
X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)

# Training set: 55,775 balanced samples (50% fraud)
# Validation set: 10,000 samples at natural 3% fraud rate
```

Applying SMOTE after the train/val split is critical. Applying it before the split would cause synthetic samples from the training set to "leak" into the validation set, producing an optimistically biased AUC estimate.

### Optuna Hyperparameter Search

```python
def objective(trial):
    params = {
        "max_depth": trial.suggest_int("max_depth", 3, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
        "n_estimators": trial.suggest_int("n_estimators", 100, 1000),
        "subsample": trial.suggest_float("subsample", 0.5, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
        "use_label_encoder": False,
        "eval_metric": "auc",
        "tree_method": "hist",   # CPU-efficient
    }
    model = XGBClassifier(**params)
    model.fit(X_train_resampled, y_train_resampled,
              eval_set=[(X_val, y_val)], early_stopping_rounds=20,
              verbose=False)
    return roc_auc_score(y_val, model.predict_proba(X_val)[:, 1])

study = optuna.create_study(direction="maximize", sampler=TPESampler(seed=42))
study.optimize(objective, n_trials=50, show_progress_bar=True)
```

### Model Registry and Versioning

```
/app/models/registry/
  2026-02-25T02-00-00/
    model.pkl           -- Serialized XGBoost model (joblib)
    metadata.json       -- {auc, n_features, train_rows, timestamp, optuna_params}
    feature_names.json  -- Ordered list of 25 feature names
    shap_values.pkl     -- SHAP values for the validation set (for explainability)
  2026-02-24T02-00-00/
    ...
  current -> 2026-02-25T02-00-00/  -- symlink to latest promoted model
```

The FastAPI server loads from `current/model.pkl` at startup. The `/model/reload` endpoint atomically updates the symlink and reloads the model into memory without restarting the server.

### Quality Gates Implementation

```python
def evaluate_model_quality(model, X_val, y_val, feature_names):
    y_pred_proba = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, y_pred_proba)

    # Gate 1: ROC-AUC threshold
    if auc < 0.85:
        raise ValueError(f"Quality gate FAILED: ROC-AUC {auc:.4f} < 0.85 threshold")

    # Gate 2: Feature count validation
    if len(feature_names) != 25:
        raise ValueError(f"Quality gate FAILED: Expected 25 features, got {len(feature_names)}")

    # Gate 3: Training data row count
    if len(X_val) < 200:  # 20% of minimum 1000 training rows
        raise ValueError(f"Quality gate FAILED: Validation set too small ({len(X_val)} rows)")

    return {"auc": auc, "gates_passed": True}
```

---

## Failure Handling

### Kafka Consumer Group Rebalancing

When a Spark executor fails and restarts, the Kafka consumer group (`spark-bronze-consumer`) triggers a rebalance. Partition reassignment takes 10-30 seconds (configurable via `session.timeout.ms`). During rebalance, no messages are consumed. After rebalance:
- Spark resumes from the Delta Lake checkpoint offset (not Kafka's stored offset)
- The `maxOffsetsPerTrigger` limit ensures the catch-up batch does not OOM the executor

### Spark Checkpointing

Bronze Structured Streaming checkpoints contain:
1. Kafka offsets for each partition at the last committed micro-batch
2. Delta Lake transaction IDs that were written in that micro-batch

If Spark fails mid-write (after some files written but before Delta commit):
1. Delta's transaction log is not updated (atomicity)
2. Written Parquet files are "orphaned" — visible on MinIO but not referenced in the log
3. On restart, Spark reads checkpoint, determines last committed offset, reprocesses the same batch
4. Delta MERGE ensures no duplicates (for Silver/Gold); Delta append ignores the orphaned files (for Bronze, duplicate Kafka offsets would create duplicates — Bronze uses Kafka offset as a dedup key in the ingested record)

### Delta Lake Transaction Log Recovery

If MinIO becomes temporarily unavailable during a write:
1. Delta attempts to write the transaction log entry
2. If MinIO is unavailable, the write fails with an S3Exception
3. Delta rolls back: no data is committed
4. Spark marks the micro-batch as failed
5. Spark retries (up to `spark.streaming.stopGracefullyOnShutdown` retries)
6. When MinIO recovers, the next retry succeeds

### Airflow Retries and Compensation

Airflow tasks that fail after partial completion:
1. `retries=3` causes automatic retry after `retry_delay=5 minutes`
2. Because Silver uses Delta MERGE (idempotent), a retry reprocesses the same partition cleanly
3. Because Gold uses Delta MERGE + JDBC truncate-insert per partition, a retry is also idempotent
4. ExternalTaskSensors do not retry independently — they wait for the upstream task to succeed on its own retry

### dbt Retry Behavior

dbt transformations fail atomically at the model level. If `marts.fact_transactions` fails:
1. The table remains in its previous state (dbt uses `CREATE TABLE AS SELECT` in a transaction)
2. The Airflow task fails
3. On retry, dbt re-runs the failed model and all downstream models

---

## Backpressure Mechanisms

| Layer | Backpressure Mechanism | Configuration |
|-------|----------------------|---------------|
| Kafka producers | Per-partition `buffer.memory` and `linger.ms` | `buffer.memory=33554432`, `linger.ms=5` |
| Spark Bronze streaming | `maxOffsetsPerTrigger` | 10,000 per partition per micro-batch |
| Spark Silver/Gold | Airflow schedule prevents overlap | `max_active_runs=1` per DAG |
| PostgreSQL writes | Spark JDBC `batchsize` | `batchsize=10000` (configurable) |
| ML Server inference | FastAPI async with worker limit | `workers=2`, `--limit-concurrency=100` |
| Airflow task slots | `max_active_tasks_per_dag=4` | LocalExecutor pool |

The primary backpressure lever is `maxOffsetsPerTrigger`. When the data generator spikes (simulated by temporarily increasing the event rate), Spark Bronze continues processing at the same rate — the only effect is increased Kafka consumer lag, which triggers a Prometheus alert (`kafka_consumer_lag > 50000`).

---

## Partitioning Strategy

All Delta tables (Bronze, Silver, Gold) are partitioned by `event_date`:

```
s3a://lakehouse/bronze/orders/
  event_date=2026-02-23/    (yesterday — complete, immutable)
  event_date=2026-02-24/    (yesterday — complete, immutable)
  event_date=2026-02-25/    (today — actively being written)
```

**Rationale**:
1. **Partition pruning**: Airflow tasks pass `--date {{ ds }}` so Spark only reads the relevant partition. A query touching one day's data reads ~1/30th of a monthly dataset.
2. **Incremental processing**: Silver and Gold transformations process only new partitions, not the full history.
3. **Retention management**: Dropping a date partition from Bronze is a single metadata operation (`ALTER TABLE DROP PARTITION`).
4. **Time-based access patterns**: Nearly all analytical queries filter by date range — partition pruning eliminates file scans dramatically.

**PostgreSQL partitioning**: `marts.fact_transactions` is range-partitioned by `date_key` with monthly child tables. Each monthly partition is created by the Airflow `gold_aggregations` DAG before writing.

---

## Scalability Bottlenecks

### Current Bottlenecks (50 events/sec)

| Component | Current Limit | Scaling Action |
|-----------|--------------|---------------|
| Kafka | 3 partitions/topic | Increase partitions; add brokers |
| Spark standalone | 2 workers × 4 cores | Add workers (Docker Compose scale) |
| MinIO single node | ~500 MB/s write throughput | MinIO distributed or S3 |
| PostgreSQL connections | 100 max connections | PgBouncer; read replicas |
| Airflow LocalExecutor | ~4 concurrent tasks | CeleryExecutor; KubernetesExecutor |

### At 100x Scale (5,000 events/sec = ~430M events/day)

At this scale, the architecture requires the following changes:

1. **Kafka**: 12 partitions per topic, 3 brokers (or Amazon MSK m5.xlarge)
2. **Spark**: EMR on EC2 m5.2xlarge (4 core, 32 GB), auto-scaling EMR clusters
3. **Delta Lake**: S3 with S3 Inventory for Delta log management; scheduled OPTIMIZE daily
4. **PostgreSQL**: Migrate to Amazon Redshift RA3 or BigQuery for sub-second aggregation queries over 400M+ rows
5. **Airflow**: Amazon MWAA with 10+ worker nodes; CeleryExecutor
6. **ML serving**: SageMaker endpoint with auto-scaling (min 2, max 10 instances)
7. **Data quality**: Integrate Great Expectations with Databricks or Glue Data Quality for distributed validation

### The Delta Lake Small File Problem

At high write frequency (every 5 minutes), Bronze partitions accumulate many small Parquet files (one per micro-batch per partition). Reading a partition with 1,000 small files is 10-100x slower than reading one optimally-sized file. Mitigation:

```python
# Run weekly via Airflow (not yet implemented — see Known Limitations)
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "s3a://lakehouse/bronze/orders/")
dt.optimize().executeCompaction()       # Merge small files
dt.vacuum(retentionHours=168)           # Remove orphaned files (keep 7 days)
```

---

## Monitoring and Alerting

### Prometheus Scrape Targets

```yaml
# prometheus.yml excerpt
scrape_configs:
  - job_name: kafka
    static_configs: [{targets: ["kafka:9101"]}]   # JMX exporter
    metrics_path: /metrics

  - job_name: spark
    static_configs: [{targets: ["spark-master:4040"]}]   # Spark metrics sink

  - job_name: airflow
    static_configs: [{targets: ["airflow-webserver:8125"]}]  # StatsD exporter

  - job_name: postgres
    static_configs: [{targets: ["postgres-exporter:9187"]}]

  - job_name: ml-server
    static_configs: [{targets: ["ml-server:8000"]}]
    metrics_path: /metrics
```

### Key Metrics and Alert Thresholds

| Metric | Alert Threshold | Severity | Response |
|--------|----------------|----------|----------|
| `kafka_consumer_lag` | > 50,000 messages | WARNING | Investigate Spark job health |
| `kafka_consumer_lag` | > 500,000 messages | CRITICAL | Page on-call; potential data loss risk |
| `spark_job_duration_seconds` | > 1800 (30 min) | WARNING | Bronze job taking too long; check executor logs |
| `airflow_dag_run_duration_seconds{dag_id="silver_transformations"}` | > 3600 | WARNING | Silver SLA breach |
| `delta_merge_duration_seconds` | > 600 | WARNING | Investigate partition size or skew |
| `ml_prediction_latency_p95` | > 500ms | WARNING | ML server under load; check batch queue |
| `great_expectations_failed_expectations` | > 0 | CRITICAL | Data quality breach; block Gold promotion |
| `postgres_connections` | > 80 | WARNING | Approaching connection limit; add PgBouncer |

### Grafana Dashboards

1. **Platform Overview**: Service health status, event throughput, Kafka consumer lag trend, last successful DAG run per DAG
2. **Data Pipeline SLAs**: Bronze/Silver/Gold processing latency, partition freshness, dbt run durations
3. **ML Model Health**: Prediction latency (p50/p95/p99), model version in production, prediction volume, fraud rate trend
4. **Data Quality**: Great Expectations validation results per suite, failed expectation history, Silver row counts over time
5. **Infrastructure**: Spark worker CPU/memory utilization, MinIO disk usage, PostgreSQL query performance, Docker container resource usage

---

## Known Limitations and Future Improvements

### Current Limitations

| Limitation | Impact | Priority |
|------------|--------|----------|
| No Delta OPTIMIZE/VACUUM scheduling | Small file accumulation degrades read performance after ~30 days | High |
| No Kafka replication (factor=1) | Broker failure causes data loss | High (production blocker) |
| No SSL/TLS on any service | All traffic in plaintext on Docker network | High (production blocker) |
| SMOTE on full training set | At very large scale (>10M rows), SMOTE is memory-intensive | Medium |
| No model A/B testing | New model is promoted with 100% traffic immediately | Medium |
| No streaming Silver joins | Silver is batch; joins happen 30min after Bronze ingest | Medium |
| No data catalog integration | Table schemas not discoverable via Hive/Glue catalog | Low |
| No GDPR delete support | Bronze is append-only; individual deletions not possible | Low |

### Planned Improvements

1. **Real-time Silver with Spark Structured Streaming**: Move Silver enrichment from 30-minute batch to a continuous streaming job, reducing end-to-end latency from 30-60 minutes to 5-10 minutes.

2. **Delta OPTIMIZE scheduled task**: Add an Airflow DAG that runs `OPTIMIZE` + `VACUUM` weekly on all Bronze and Silver tables to prevent small file degradation.

3. **Model A/B testing**: Implement a canary deployment pattern for the ML server — new model receives 10% of traffic; compare fraud precision/recall over 24 hours before full promotion.

4. **Great Expectations integration with Airflow**: Move from standalone `DataQualityRunner` to Airflow-native Great Expectations operator so validation failures block downstream DAG runs via the standard Airflow dependency mechanism.

5. **Unity Catalog / Hive Metastore**: Register all Delta tables in a catalog for schema discoverability, access control, and cross-team data sharing.

6. **Streaming ML inference**: Add a Kafka Streams or Flink job that applies the fraud model inline with the payment event stream, enabling real-time transaction blocking (sub-second latency from event to fraud decision).

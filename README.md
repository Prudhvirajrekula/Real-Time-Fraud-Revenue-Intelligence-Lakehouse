# Real-Time Fraud & Revenue Intelligence Platform

A production-mirroring lakehouse stack: six Kafka topics → Spark Structured Streaming → Bronze/Silver/Gold Delta Lake on MinIO → PostgreSQL analytics warehouse → dbt star schema + XGBoost fraud detection served via FastAPI. Runs entirely on a single machine via Docker Compose. Designed to expose and solve the integration problems that emerge at every layer boundary — not to paper over them.

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Component Design & Decisions](#component-design--decisions)
4. [Data Model](#data-model)
5. [ML Pipeline](#ml-pipeline)
6. [Orchestration & Dependency Model](#orchestration--dependency-model)
7. [Failure Modes & Recovery](#failure-modes--recovery)
8. [Scalability](#scalability)
9. [Operational Runbook](#operational-runbook)
10. [Testing Strategy](#testing-strategy)
11. [Production Gap Analysis](#production-gap-analysis)

---

## System Overview

### Scope

This platform ingests synthetic transactional event streams across six domains (orders, payments, users, devices, geo-events, refunds), transforms them through a three-layer Delta Lake medallion architecture, computes fraud risk scores and revenue KPIs, and serves real-time ML predictions.

**Throughput**: ~50 events/sec (~4.3M events/day) from the synthetic data generator. Architecture is partitioned and parameterized to scale to ~120 events/sec (10M events/day) through horizontal Spark and Kafka scaling with no code changes.

**Latency profile**: Batch-oriented. Bronze ingest latency: ≤30 min. Silver enrichment latency: ≤60 min end-to-end from event time. Gold aggregations: ≤2 hours. ML predictions via `/predict`: <50ms p99 (single-transaction, model already loaded).

**Consistency model**: Delta Lake provides serializable isolation on writes. Spark MERGE upserts on Silver make all transformations idempotent — any partition can be reprocessed any number of times from Bronze without producing duplicates. Airflow retries are expected behavior, not exceptional.

### Non-Goals

- **Sub-second streaming latency**: The architecture is micro-batch (30-min trigger), not continuous streaming. Fraud scoring at transaction time uses the pre-computed ML model endpoint, not re-running Spark on each event.
- **Multi-region replication**: MinIO runs as a single-node instance. In production, this maps to S3 with cross-region replication enabled.
- **Feature store service**: Silver Delta tables serve as the feature store. Point-in-time correctness is achieved via event_date partitioning and time-travel queries on Delta. A dedicated feature store (Feast, Hopsworks) is unnecessary at this scale.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION                                                                  │
│                                                                             │
│  Python Data Generator (~50 events/sec)                                    │
│  6 Confluent Kafka producers w/ Schema Registry (BACKWARD compatibility)   │
│                                                                             │
│  Topics: orders │ payments │ users │ devices │ geo_events │ refunds         │
│  6 partitions each │ 7-day retention │ replication.factor=1 (local)        │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │  Kafka consumer group: spark-bronze
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PROCESSING  — Apache Spark 3.5.5 Standalone (1 master, 2 workers)         │
│              — Delta Lake 2.4.0   — MinIO (S3A, s3a://fraud-platform/)     │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │ BRONZE  s3a://fraud-platform/bronze/{topic}/event_date=YYYY-MM-DD  │    │
│  │ Append-only. Raw JSON preserved. No business logic.                │    │
│  │ Schema coercion: timestamp parse + topic tag. Kafka offset tracked.│    │
│  └──────────────────────────────────┬─────────────────────────────────┘    │
│                                     │  Airflow: bronze_ingestion (30 min)  │
│                                     ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │ SILVER  s3a://fraud-platform/silver/                               │    │
│  │ Cross-topic joins. Type validation. 25 ML feature columns.         │    │
│  │ MERGE upsert on (order_id, event_date) → idempotent reruns.       │    │
│  │ Great Expectations validation gates Silver → Gold promotion.       │    │
│  └──────────────────────────────────┬─────────────────────────────────┘    │
│                                     │  Airflow: silver_transformations     │
│                                     ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │ GOLD  s3a://fraud-platform/gold/                                   │    │
│  │ Daily/hourly aggregations. Fraud KPIs. Revenue metrics.            │    │
│  │ Z-ordered on (shipping_country, event_date). JDBC → PostgreSQL.   │    │
│  └──────────────────────────────────┬─────────────────────────────────┘    │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       │  Airflow: gold_aggregations (2 hr)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  WAREHOUSE  — PostgreSQL 15                                                 │
│                                                                             │
│  Schemas: gold (Spark writes) │ silver │ staging │ marts │ intermediate    │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │  dbt-postgres 1.7 — 8 models — 34 schema tests                  │      │
│  │  gold.* → staging.stg_* → marts.fact_orders                     │      │
│  │                         → marts.fact_fraud_events                │      │
│  │                         → marts.dim_users                        │      │
│  │                         → marts.dim_dates                        │      │
│  └──────────────────────────────────────────────────────────────────┘      │
└──────────┬──────────────────────────────────────────────────────────────────┘
           │
     ┌─────┴──────┐          ┌─────────────────────────────────┐
     │ ML SERVING │          │  OBSERVABILITY                  │
     │            │          │                                 │
     │ FastAPI     │          │  Prometheus (scrape: all svcs)  │
     │ XGBoost 2.0 │          │  Grafana (Kafka lag, Spark      │
     │ /predict    │          │  throughput, ML p99 latency,    │
     │ /predict/   │          │  dbt run durations, DQ pass     │
     │  batch      │          │  rates)                         │
     │ /model/     │          │                                 │
     │  reload     │          │  postgres_exporter + kafka-     │
     └────────────┘          │  exporter + spark metrics sink  │
                             └─────────────────────────────────┘

  ORCHESTRATION: Airflow 2.8.1 (LocalExecutor)
  6 DAGs with ExternalTaskSensor cross-DAG dependency enforcement
  bronze_ingestion → silver_transformations → gold_aggregations
  → dbt_transformations → data_quality (hourly) │ ml_fraud_training (daily)
```

---

## Component Design & Decisions

### Kafka: Topic Partitioning and Consumer Isolation

Each topic is provisioned with **6 partitions**. This is deliberate: with 2 Spark workers and 3 executor cores each, 6 partitions maps evenly to 6 Spark tasks per micro-batch — no partition sits idle, no executor is oversubscribed. Adding a third Spark worker without touching the producer requires only increasing Spark's `spark.default.parallelism`.

Consumer group `spark-bronze-consumer` is dedicated to the Bronze ingest job. `spark-silver-consumer` is reserved for any future Silver streaming job. Consumer group isolation prevents offset management for one job from interfering with another's checkpoint state — a common operational failure mode when teams share consumer groups.

Schema Registry is configured with `BACKWARD` compatibility mode per topic: new schema versions may add optional fields, but existing consumer code reading against the previous schema must not break. This enforces a contract at the wire level rather than relying on application-level defensive coding.

### Delta Lake: ACID, MERGE, and Checkpointing

Bronze uses **append-only writes** (`mode="append"`). The Delta transaction log tracks exactly which Kafka offsets have been committed. If the Spark job crashes mid-write, the partial transaction is rolled back on next startup — no partial files are visible to downstream readers.

Silver uses **MERGE upserts** (`MERGE INTO silver.orders_enriched USING updates ON (order_id, event_date)`). This is a strict requirement, not a convenience: Airflow's retry behavior means any task may execute more than once. An `INSERT OVERWRITE` would re-create the partition on retry, potentially losing concurrent writes from adjacent date partitions. A `MERGE` is idempotent.

**`autoOptimize.optimizeWrite`** is enabled globally: Delta coalesces small files produced by Spark's partition shuffle before committing, reducing the small-file problem without a separate `OPTIMIZE` job run. At higher write frequencies, explicit `OPTIMIZE` + `ZORDER` on a weekly schedule is still required to consolidate accumulated files.

**Checkpoint location**: `s3a://fraud-platform/checkpoints/bronze/{topic}/`. Spark Structured Streaming writes its offset and metadata to this path after each successful micro-batch commit. On restart, Spark resumes from the last committed offset — no duplicate processing, no data loss, no manual offset management.

### Spark: AQE, Broadcast Joins, and Memory Tuning

**Adaptive Query Execution** (`spark.sql.adaptive.enabled=true`) is enabled for all jobs. At runtime, AQE replaces pre-planned shuffle partitions with the actual data distribution, splits skewed partitions (common when a single merchant_id dominates a partition), and converts sort-merge joins to broadcast joins when the smaller table drops below `spark.sql.autoBroadcastJoinThreshold` (default 10MB).

The Silver enrichment job performs three joins: orders × payments (on order_id), × devices (on user_id + session_id), × geo_events (on user_id + timestamp window). The payments, devices, and geo_events tables for a single 30-minute window are small enough (< 5MB) to broadcast in development; they would exceed broadcast threshold at 10M+ events/day and require a partitioned sort-merge join instead.

`spark.sql.shuffle.partitions=50` is set below the default of 200. At ~50 events/sec the data volume per shuffle stage is measured in megabytes, not gigabytes. 200 shuffle partitions would produce 200 files of a few KB each — pathological for Delta write performance. 50 balances task granularity against write amplification at this scale.

### PostgreSQL: Schema Isolation and Write Strategy

Five schemas serve distinct purposes:

| Schema | Writer | Reader | Purpose |
|--------|--------|--------|---------|
| `gold` | Spark (JDBC) | dbt, Grafana | Aggregation tables written by Spark gold jobs |
| `silver` | Spark (JDBC) | ML training | Feature tables for model training (copied from Delta) |
| `staging` | dbt | dbt | Staging views over gold sources |
| `marts` | dbt | BI tools | Star schema fact and dimension tables |
| `intermediate` | dbt | dbt | Intermediate transforms between staging and marts |

Spark writes to `gold.*` via JDBC with `mode="overwrite"` and explicit partition-level replacement. dbt reads only from `gold.*` and writes only to `staging.*`, `marts.*`, and `intermediate.*`. This schema boundary prevents Spark and dbt from contending on the same tables and makes the lineage auditable from the schema name alone.

### Airflow: Custom Dockerfile for Dependency Isolation

The stock `apache/airflow:2.8.1-python3.11` image ships with a system-level `airflow` package. Installing `apache-airflow-providers-apache-spark` via pip at runtime creates a namespace package conflict: Python's import system finds `airflow.providers` under the system install path and stops searching, so the user-installed providers at `~/.local/lib/` are never discovered. This manifests as `ModuleNotFoundError: No module named 'airflow.providers.apache'` at DAG parse time — an import error that passes `pip install` silently but fails at runtime.

The fix is a **custom `airflow/Dockerfile`** that installs all providers at image build time against the same Python environment as the base image. `docker compose build` produces an image where providers, dbt, and Great Expectations are all installed under the same `site-packages` path, eliminating the split-install conflict entirely.

This is the canonical pattern for Airflow dependency management in any containerized deployment — not `pip install` in the entrypoint.

### ML Serving: Hot-Reload Without Downtime

The FastAPI ML server loads the model from `MODEL_REGISTRY_PATH` at startup and exposes a `/model/reload` endpoint that swaps the in-memory model reference atomically without restarting the process. The Airflow `ml_fraud_training` DAG calls this endpoint as its final task after saving the new model version to the registry. There is no service restart, no dropped connections, and no window where the server returns 503.

The model registry stores one directory per version (named by training timestamp: `20260226_011418/`). The `latest/` symlink points to the most recent passing version. The server follows this symlink on reload, meaning rollback is a symlink swap.

---

## Data Model

### Bronze (raw ingest)

```
Partition: event_date (DATE)
Granularity: one row per Kafka message

Columns common to all topics:
  _kafka_topic      STRING    -- topic name for cross-topic debugging
  _kafka_partition  INT       -- partition number
  _kafka_offset     LONG      -- offset for exactly-once tracking
  _ingested_at      TIMESTAMP -- wall clock at Spark ingest time
  event_date        DATE      -- derived from _ingested_at, used for partitioning

Topic-specific payload: raw JSON string preserved as-is.
No field parsing, no type coercion, no business logic at this layer.
```

### Silver (enriched + ML features)

```
Table: silver.orders_enriched
Partition: event_date (DATE)
MERGE key: (order_id, event_date)

Core fields (from orders + payments join):
  order_id, user_id, payment_method, total_amount, currency,
  order_status, payment_outcome, created_ts

Enrichment fields (from devices + geo_events join):
  device_id, device_type, device_os, vpn_detected, bot_score,
  shipping_country, billing_country, geo_mismatch_flag

ML feature columns (25 total):
  velocity_1h, velocity_24h, velocity_7d     -- transaction velocity per user
  amount_vs_user_avg, amount_vs_user_p95     -- amount deviation from baseline
  account_age_days                            -- user tenure signal
  chargeback_count, chargeback_rate          -- historical chargeback signal
  refund_count_30d, fraud_refund_count       -- refund pattern signal
  vpn_detected, proxy_detected,
  tor_detected, bot_detected                 -- device risk signals
  device_count_30d                           -- device diversity per user
  geo_mismatch, is_high_risk_country,
  country_risk_score                         -- geo risk signals
  payment_risk_score, is_3ds_authenticated   -- payment method risk
  risk_tier_encoded                          -- bucketed risk tier (0/1/2)
  hour_of_day, day_of_week, is_weekend       -- temporal features
  label                                      -- 0/1 fraud ground truth
```

### Gold (aggregations)

```
Table: gold.revenue_daily
Partition: event_date (DATE)
Writer: Spark gold job (mode=overwrite, partition_cols=[event_date])

  event_date, shipping_country, currency, payment_method, amount_tier
  total_orders, gmv, net_revenue, fraud_amount
  fraud_orders, failed_payments, unique_customers
  fraud_rate, payment_failure_rate
  avg_order_value, median_order_value, p95_order_value
  _computed_at TIMESTAMP

Table: gold.fraud_summary
Partition: event_date (DATE)

  event_date, shipping_country, payment_method, amount_tier
  total_orders, fraud_orders, fraud_gmv, total_gmv
  fraud_rate, fraud_gmv_rate
  geo_mismatch_count, vpn_orders
  total_refunds, total_refund_amount, fraud_refunds
  _computed_at TIMESTAMP

Table: gold.user_fraud_scores
No partition (full overwrite on each run, ~30d rolling window)

  user_id
  orders_30d, fraud_count_30d
  avg_risk_score, avg_velocity_24h
  vpn_sessions_30d, geo_mismatches_30d, avg_amount_deviation
  user_fraud_rate, composite_risk_score
  risk_label  -- 'low' / 'medium' / 'high'
  _computed_at TIMESTAMP
```

### dbt Star Schema (marts)

```
Fact tables (views over gold + staging joins):
  marts.fact_orders       -- grain: one row per order
  marts.fact_fraud_events -- grain: one row per fraud event

Dimension tables:
  marts.dim_users         -- user risk profile + segmentation
  marts.dim_dates         -- date spine 2023-01-01 to current_date+1yr
                             (generate_series, not dbt_utils.date_spine)
```

---

## ML Pipeline

### Feature Selection Rationale

Features fall into four behavioral categories, each capturing a distinct fraud signal:

| Category | Features | Signal |
|----------|----------|--------|
| Velocity | `velocity_1h`, `velocity_24h`, `velocity_7d` | Burst behavior preceding account takeover or carding attacks |
| Amount deviation | `amount_vs_user_avg`, `amount_vs_user_p95` | Single large transaction deviating from user baseline |
| Account history | `account_age_days`, `chargeback_count`, `chargeback_rate` | New accounts and prior fraud history as persistent risk signals |
| Device + geo | `vpn_detected`, `tor_detected`, `geo_mismatch`, `device_count_30d` | Infrastructure-level anomalies correlated with fraud operations |

### SMOTE Application

SMOTE is applied **after the train/validation split**, on the training set only. This is a strict data hygiene requirement: applying SMOTE before splitting creates synthetic samples that are statistically correlated with real validation samples, producing optimistically biased AUC estimates. The implementation in `ml/models/fraud_detector.py` enforces this ordering.

```
Train (80%): 40,000 samples @ 3% fraud → SMOTE → ~46,000 balanced samples
Validation (20%): 10,000 samples @ 3% fraud (natural class ratio, untouched)
```

### Quality Gates

A model version is only promoted to `latest/` if all of the following pass:

| Gate | Threshold | Rationale |
|------|-----------|-----------|
| ROC-AUC ≥ 0.85 | Hard block | Below this, model is not meaningfully discriminating |
| Precision ≥ 0.70 | Hard block | Controls false positive rate (legitimate transactions declined) |
| Recall ≥ 0.60 | Hard block | Controls false negative rate (fraud missed entirely) |
| Feature count == 25 | Hard block | Detects silent schema drift between training and serving |
| Training rows ≥ 1,000 | Hard block | Prevents training on empty or near-empty partition |

On synthetic data, ROC-AUC reaches 1.0 — expected, because synthetic fraud has perfectly separable feature distributions by construction. On real transaction data with 3-5% label noise and behavioral overlap, expected AUC is 0.90–0.96. The quality gates are calibrated for real-world performance, not synthetic.

### Serving

`POST /predict` accepts a JSON payload with all 25 feature values and returns:

```json
{
  "fraud_probability": 0.83,
  "is_fraud": true,
  "risk_level": "high",
  "model_version": "20260226_011418",
  "latency_ms": 12
}
```

`POST /predict/batch` accepts a list of transactions and returns predictions for all in a single model call. Batch inference is ~40x faster per transaction than calling `/predict` in a loop due to vectorized XGBoost inference.

---

## Orchestration & Dependency Model

```
bronze_ingestion (every 30 min)
  └─ reads Kafka → writes Delta bronze

silver_transformations (every 30 min)
  └─ ExternalTaskSensor(bronze_ingestion.end)
  └─ reads bronze Delta → enriches → writes silver Delta

gold_aggregations (every 2 hr)
  └─ ExternalTaskSensor(silver_transformations.end)
  └─ reads silver Delta → aggregates → writes gold Delta + PostgreSQL

dbt_transformations (every 2 hr)
  └─ ExternalTaskSensor(gold_aggregations.end)
  └─ dbt run + dbt test against PostgreSQL gold schema

data_quality (hourly, at :45)
  └─ runs independently (validates PostgreSQL gold tables)
  └─ check_freshness → run_great_expectations → check_row_counts

ml_fraud_training (daily, 02:00 UTC)
  └─ reads silver Delta (30d rolling) → trains XGBoost
  └─ evaluates quality gates
  └─ saves to registry → POST /model/reload
```

**DAG granularity rationale**: Each layer is a separate DAG rather than one monolithic DAG. This allows independent retry policies per layer (Bronze failures don't force Silver reruns), separate on-call ownership (pipeline engineers own Bronze/Silver, analytics engineers own dbt), and independent scheduling cadences. The ExternalTaskSensor with `mode="reschedule"` handles cross-DAG sequencing without holding a worker slot while waiting.

---

## Failure Modes & Recovery

| Component | Failure Mode | Detection | Recovery |
|-----------|-------------|-----------|----------|
| Kafka broker | Broker unavailable | Spark job fails on connect; Airflow task retries | Airflow retries 3× at 5-min intervals; backlog drains on recovery |
| Spark driver crash | Mid-write failure | Delta transaction rolled back; partial files invisible to readers | Spark reads last committed checkpoint offset on restart; reprocesses from that point |
| Silver MERGE conflict | Two concurrent writes to same partition | Delta optimistic concurrency control serializes writes; second writer retries automatically | Delta guarantees serializable isolation; no data corruption |
| Airflow scheduler restart | In-flight task state lost | Tasks left in "running" state in metadata DB | Airflow auto-clears zombie tasks after `task_instance_mutation_hook` timeout |
| PostgreSQL unavailable | JDBC write fails | Spark job task fails | Airflow retries; Delta Gold table still updated; PostgreSQL write retried on next run |
| ML server OOM | Model inference fails | FastAPI returns 500; Prometheus alert on error rate | Kubernetes restarts container (or Docker Compose restart policy); `/predict` returns 503 during restart |
| Data quality gate failure | Silver data fails GE checks | `data_quality` DAG task marked `FAILED`; row written to `validation_results` table | Pipeline does not advance to Gold; engineer reviews failed expectations; Bronze partition is source of truth for replay |

### Exactly-Once Semantics

Bronze writes are **at-least-once** at the Kafka consumer level, with **exactly-once** at the Delta level via the transaction log. The sequence:

1. Spark micro-batch reads Kafka offsets `[O₁, O₂)`
2. Spark writes files to `_delta_log/` staging location
3. Delta transaction commit atomically advances the committed offset in `_delta_log/0000N.json`
4. On next run, Spark reads the committed offset from the checkpoint store and begins at `O₂`

If the job crashes between steps 2 and 3, the staged files are abandoned (not committed), and step 1 repeats from `O₁` on restart — producing duplicate ingestion of the same offsets, which the Silver MERGE deduplicates by key.

---

## Scalability

### Kafka → 10M Events/Day

At 115 events/sec sustained (10M/day), the current 6-partition topology is the first bottleneck. Each partition receives ~19 events/sec; a single Spark executor with one core can consume this comfortably. However, adding Spark workers without increasing partition count yields diminishing returns: `min(tasks, partitions)` limits parallelism.

Recommended action: increase to **12 partitions per topic** (allows 12 parallel Spark tasks per micro-batch), matching a 4-worker Spark cluster with 3 cores each.

### Spark Write Amplification

At 10M events/day with 30-minute micro-batches, each Bronze micro-batch writes ~208,000 events. With `spark.sql.shuffle.partitions=50`, each table write produces up to 50 Parquet files per partition. At 48 micro-batches/day × 50 files = **2,400 files per day_partition**. This is the small-file problem.

Mitigations already in place:
- `autoOptimize.optimizeWrite=true`: Delta coalesces small files at write time
- `autoCompact.enabled=true`: Delta triggers background compaction when file count exceeds threshold

Additional mitigation at scale: weekly scheduled `OPTIMIZE ... ZORDER BY (shipping_country, payment_method)` + `VACUUM` to remove obsolete versions older than 7 days.

### PostgreSQL at 10M Events/Day

At this volume, `gold.revenue_daily` receives daily partition overwrites of ~50-200K rows. The current single-instance PostgreSQL 15 handles this. Above ~500M total rows, table partitioning by `event_date` (already the partition scheme in Spark) should be reflected in PostgreSQL as declarative table partitioning to maintain query performance on time-range scans.

For 100M+ events/day or sub-second query SLAs on aggregations, migrate the warehouse to a columnar store (Redshift RA3, BigQuery, or Snowflake). The dbt models are written with standard SQL and are compatible with all three adapters without modification.

---

## Operational Runbook

### Prerequisites

| Requirement | Minimum |
|-------------|---------|
| RAM | 12 GB allocated to container runtime |
| Disk | 60 GB free (MinIO data + Delta logs + Postgres WAL) |
| CPU | 4 cores (6-8 for comfortable Spark parallelism) |
| macOS | Colima with `--vm-type vz` (Apple Silicon) |

### Startup

```bash
# macOS: start Colima with sufficient resources
colima start --cpu 6 --memory 12 --disk 60 --arch aarch64 --vm-type vz

# Clone and configure
git clone https://github.com/your-org/fraud-intelligence-platform.git
cd fraud-intelligence-platform
cp .env.example .env   # review defaults; do not commit .env

# Start all services
docker compose up -d

# Verify readiness (~2-3 min for all health checks to pass)
docker compose ps

# Run smoke tests
bash scripts/run_smoke_test.sh
# Expected: 19/19 PASS
```

### Service Endpoints

| Service | URL | Auth |
|---------|-----|------|
| Airflow | http://localhost:8086 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Kafka UI | http://localhost:8082 | None |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Spark Master UI | http://localhost:8083 | None |
| ML Server | http://localhost:8000 | None |
| ML API Docs | http://localhost:8000/docs | None |
| Grafana | http://localhost:3000 | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` |
| Prometheus | http://localhost:9090 | None |
| Schema Registry | http://localhost:8081 | None |
| PostgreSQL | localhost:5432 | `POSTGRES_USER` / `POSTGRES_PASSWORD` |

### Triggering Pipeline Manually

```bash
# Trigger individual DAGs
docker exec airflow-webserver airflow dags trigger bronze_ingestion
docker exec airflow-webserver airflow dags trigger silver_transformations
docker exec airflow-webserver airflow dags trigger gold_aggregations

# Run dbt manually
docker exec airflow-webserver \
  dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# Run ML training
docker exec ml-server python /app/models/train.py

# Check Kafka topic offsets
docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 --topic orders --time -1
```

### Observing Kafka Consumer Lag

```bash
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe --group spark-bronze-consumer
```

A lag growing over multiple micro-batch cycles indicates the Spark Bronze job is not keeping up with producer throughput. First remediation: increase `maxOffsetsPerTrigger` in the Bronze Spark job config.

---

## Testing Strategy

| Layer | Test Type | Count | Runner |
|-------|-----------|-------|--------|
| Data generator | Unit (generators, field validation, fraud flag logic) | 26 | `pytest` in `data-generator` container |
| ML model | Unit (training, serialization, prediction range, feature importance) | 10 | `pytest` in `ml-server` container |
| Data quality | Unit (ValidationResult class) | 3 | `pytest` in `airflow-webserver` container |
| dbt models | Schema tests (not_null, unique, accepted_values, relationships) | 34 | `dbt test` in `airflow-webserver` container |
| Infrastructure | Smoke (service reachability, Kafka topics, MinIO buckets) | 19 | `bash scripts/run_smoke_test.sh` |

**Total**: 92 automated checks, 0 failures on the validated build.

Integration tests (`tests/integration/`) require the full platform running and are excluded from the default test suite (`pytest -m "not integration"`). They validate end-to-end data flow: producer → Kafka → Bronze Delta → Silver enrichment → Gold aggregation → PostgreSQL.

---

## Production Gap Analysis

The following table documents known deviations from a production-hardened deployment, with the specific remediation for each.

| Gap | Impact | Production Remediation |
|-----|--------|----------------------|
| Single Kafka broker, `replication.factor=1` | Broker failure causes message loss for in-flight data | Amazon MSK (3 brokers); `replication.factor=3`, `min.insync.replicas=2` |
| Airflow LocalExecutor | No horizontal task scaling; scheduler and worker share one process | CeleryExecutor (Redis broker) or Amazon MWAA; 4+ workers |
| No Kafka TLS/SASL | Messages transmitted in plaintext on internal Docker network | Confluent Kafka SSL listeners + keystore/truststore rotation |
| MinIO single-node | No data durability guarantees on disk failure | AWS S3 (eleven 9s durability); or MinIO distributed mode (≥4 drives) |
| No Delta OPTIMIZE schedule | Small-file accumulation degrades read performance over weeks | Weekly Airflow task: `OPTIMIZE ... ZORDER BY` + `VACUUM RETAIN 168 HOURS` |
| PostgreSQL single-instance | No failover; all Spark gold writes go to one node | Amazon RDS Multi-AZ PostgreSQL 15; PgBouncer for connection pooling |
| No secrets rotation | `.env` credentials are static; no audit trail | AWS Secrets Manager + IAM instance profiles; HashiCorp Vault for dynamic credentials |
| No CI/CD pipeline | Tests run manually | GitHub Actions: lint → unit tests → dbt tests → build → deploy; see `DEPLOYMENT.md` |
| No Kafka topic ACLs | Any service can produce/consume any topic | Confluent RBAC; per-consumer-group ACLs; producer ACLs per service identity |

See `DEPLOYMENT.md` for AWS service mappings and `SYSTEM_DESIGN.md` for architectural decision rationale in depth.

---

## License

MIT

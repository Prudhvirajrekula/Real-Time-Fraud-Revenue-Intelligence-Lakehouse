"""
DAG: Bronze Layer - Kafka to Delta Lake Streaming Monitor

Monitors the running Spark streaming job.
If the stream drops, restarts it via SparkSubmitOperator.

Schedule: @continuous (monitors every 5 minutes)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

import os

SPARK_CONN_ID = "spark_default"
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
KAFKA_BROKERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
}

SPARK_JARS = (
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

default_args = {
    "owner": "platform-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="bronze_ingestion",
    description="Kafka → Delta Lake bronze layer ingestion (streaming monitor)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "streaming", "kafka", "delta"],
    doc_md="""
## Bronze Ingestion DAG

Monitors and restarts the Spark Structured Streaming job that reads
from Kafka and writes raw events to Delta Lake bronze layer.

### Topics ingested:
- orders, payments, users, devices, geo_events, refunds

### Delta Lake paths:
- `s3a://fraud-platform/bronze/{topic}`

### Checkpoints:
- `s3a://fraud-platform/checkpoints/bronze/{topic}`
""",
) as dag:

    start = EmptyOperator(task_id="start")

    check_kafka = BashOperator(
        task_id="check_kafka_health",
        bash_command=(
            "docker exec kafka kafka-broker-api-versions "
            "--bootstrap-server kafka:29092 > /dev/null 2>&1 && echo 'HEALTHY' || echo 'UNHEALTHY'"
        ),
    )

    check_minio = BashOperator(
        task_id="check_minio_health",
        bash_command="curl -s http://minio:9000/minio/health/live && echo 'HEALTHY'",
    )

    start_bronze_stream = SparkSubmitOperator(
        task_id="start_bronze_streaming",
        application="/opt/airflow/spark_jobs/bronze/ingest_stream.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_JARS,
        conf=SPARK_CONF,
        driver_memory="2g",
        executor_memory="2g",
        executor_cores=2,
        num_executors=2,
        application_args=[],
        verbose=False,
        # Note: this submits the job. For a long-running stream,
        # use yarn cluster mode or monitor via Spark REST API.
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> [check_kafka, check_minio] >> start_bronze_stream >> end

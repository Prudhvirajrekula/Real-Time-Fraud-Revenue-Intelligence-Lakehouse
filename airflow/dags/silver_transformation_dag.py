"""
DAG: Silver Layer Transformations

Runs Spark batch jobs to transform bronze → silver:
1. transform_transactions.py  (orders + payments enrichment)
2. build_fraud_features.py    (ML feature engineering)

Schedule: Hourly
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

import os

SPARK_CONN_ID = "spark_default"
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")

SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
}

SPARK_PACKAGES = (
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

default_args = {
    "owner": "platform-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="silver_transformations",
    description="Bronze → Silver layer transformations (batch)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 * * * *",  # Every hour
    catchup=False,
    max_active_runs=1,
    tags=["silver", "spark", "delta", "batch"],
    doc_md="""
## Silver Transformation DAG

Reads from Bronze Delta Lake, applies data quality rules,
enrichment logic, and writes to Silver layer.

### Jobs:
1. **transform_transactions**: Cleans + enriches orders & payments
2. **build_fraud_features**: Computes ML feature vector

### SLA: Should complete within 30 minutes of hourly trigger
""",
) as dag:

    start = EmptyOperator(task_id="start")

    transform_transactions = SparkSubmitOperator(
        task_id="transform_transactions",
        application="/opt/airflow/spark_jobs/silver/transform_transactions.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf={
            **SPARK_CONF,
            "spark.executor.memory": "3g",
            "spark.driver.memory": "2g",
        },
        executor_memory="3g",
        driver_memory="2g",
        num_executors=2,
        verbose=False,
    )

    build_fraud_features = SparkSubmitOperator(
        task_id="build_fraud_features",
        application="/opt/airflow/spark_jobs/silver/build_fraud_features.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf={
            **SPARK_CONF,
            "spark.executor.memory": "4g",  # Feature engineering is memory-heavy
            "spark.sql.shuffle.partitions": "100",
        },
        executor_memory="4g",
        num_executors=2,
        verbose=False,
    )

    end = EmptyOperator(task_id="end")

    start >> transform_transactions >> build_fraud_features >> end

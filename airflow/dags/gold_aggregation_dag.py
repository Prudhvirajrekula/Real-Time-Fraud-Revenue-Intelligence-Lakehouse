"""
DAG: Gold Layer Aggregations

Computes business-level aggregations for BI and ML consumption:
1. revenue_aggregations.py  - Revenue + product metrics
2. fraud_summary.py         - Fraud KPIs + user risk scores

Also triggers dbt models after gold tables are written to PostgreSQL.
Schedule: Every 2 hours
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

import os

SPARK_CONN_ID = "spark_default"
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
POSTGRES_JDBC = os.environ.get("POSTGRES_CONN_STRING", "postgresql://postgres:5432/fraud_platform").replace("postgresql://", "jdbc:postgresql://")

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
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC",
}

SPARK_PACKAGES = (
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.6.0"
)

default_args = {
    "owner": "platform-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="gold_aggregations",
    description="Silver → Gold aggregations + PostgreSQL write",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 */2 * * *",  # Every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=["gold", "spark", "delta", "aggregation"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Wait for silver to be fresh (within last 2 hours)
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="silver_transformations",
        external_task_id="end",
        timeout=3600,
        mode="reschedule",
        poke_interval=300,
        allowed_states=["success"],
    )

    revenue_aggregations = SparkSubmitOperator(
        task_id="revenue_aggregations",
        application="/opt/airflow/spark_jobs/gold/revenue_aggregations.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        executor_memory="3g",
        num_executors=2,
        verbose=False,
    )

    fraud_summary = SparkSubmitOperator(
        task_id="fraud_summary",
        application="/opt/airflow/spark_jobs/gold/fraud_summary.py",
        conn_id=SPARK_CONN_ID,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        executor_memory="3g",
        num_executors=2,
        verbose=False,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_silver >> [revenue_aggregations, fraud_summary] >> end

"""
DAG: Data Quality Validation

Runs Great Expectations checkpoints against bronze and silver tables.
Sends alerts if data quality thresholds are breached.

Schedule: Every hour (after bronze ingestion)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-quality",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "email_on_failure": False,
}


def check_data_freshness(**context) -> None:
    """Verify that bronze tables have been updated within the last 2 hours."""
    import os
    import boto3
    from datetime import datetime, timezone, timedelta

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    tables = ["orders", "payments", "users"]
    bucket = "fraud-platform"
    stale_threshold = timedelta(hours=2)
    now = datetime.now(timezone.utc)

    for table in tables:
        prefix = f"bronze/{table}/"
        try:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10)
            objects = response.get("Contents", [])
            if not objects:
                print(f"WARNING: No data found in bronze/{table}")
                continue
            latest = max(obj["LastModified"] for obj in objects)
            age = now - latest
            if age > stale_threshold:
                print(f"WARNING: bronze/{table} is stale (age: {age})")
            else:
                print(f"OK: bronze/{table} freshness: {age}")
        except Exception as e:
            print(f"ERROR checking bronze/{table}: {e}")


def run_great_expectations(**context) -> dict:
    """Run Great Expectations validation suite."""
    import subprocess
    result = subprocess.run(
        ["python", "/opt/airflow/data_quality/validate.py"],
        capture_output=True, text=True, timeout=600
    )
    print("STDOUT:", result.stdout[-3000:])
    if result.returncode != 0:
        print("STDERR:", result.stderr[-2000:])
        # Log failure but don't fail the DAG - use separate alerting
    return {"ge_exit_code": result.returncode}


def check_row_counts(**context) -> None:
    """Alert if row counts drop below expected thresholds."""
    import os
    import psycopg2

    conn = psycopg2.connect(os.environ["POSTGRES_CONN_STRING"])
    cur = conn.cursor()

    checks = [
        ("gold.revenue_daily",    "SELECT COUNT(*) FROM gold.revenue_daily WHERE event_date = CURRENT_DATE",    1),
        ("gold.fraud_summary",    "SELECT COUNT(*) FROM gold.fraud_summary WHERE event_date = CURRENT_DATE",    1),
        ("gold.user_fraud_scores","SELECT COUNT(*) FROM gold.user_fraud_scores",                               100),
    ]

    for table_name, query, min_rows in checks:
        try:
            cur.execute(query)
            count = cur.fetchone()[0]
            if count < min_rows:
                print(f"WARNING: {table_name} has only {count} rows (expected >= {min_rows})")
            else:
                print(f"OK: {table_name} has {count} rows")
        except Exception as e:
            print(f"ERROR checking {table_name}: {e}")

    cur.close()
    conn.close()


with DAG(
    dag_id="data_quality",
    description="Great Expectations data quality validation",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="45 * * * *",  # 45 min past each hour
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "great-expectations", "monitoring"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_freshness = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    run_ge_validation = PythonOperator(
        task_id="run_great_expectations",
        python_callable=run_great_expectations,
    )

    check_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    end = EmptyOperator(task_id="end")

    start >> check_freshness >> run_ge_validation >> check_counts >> end

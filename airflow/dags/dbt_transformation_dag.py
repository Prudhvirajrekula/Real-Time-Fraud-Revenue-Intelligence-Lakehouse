"""
DAG: dbt Star Schema Transformations

Runs dbt models to build the analytics star schema in PostgreSQL:
- Staging models (views on gold tables)
- Dimension tables (users, dates)
- Fact tables (orders, fraud_events)

Runs after gold aggregations complete.
Schedule: Every 2 hours (30 min after gold)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

DBT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "analytics-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_transformations",
    description="dbt star schema transformation (gold → marts)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 */2 * * *",  # 30 min after gold aggregations
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "analytics", "star-schema", "postgres"],
    doc_md="""
## dbt Transformation DAG

Runs dbt models to build a star schema in PostgreSQL marts schema.

### Flow:
1. Wait for gold aggregations
2. Run dbt models (staging → intermediate → marts)
3. Run dbt tests
4. Generate docs

### Schemas:
- `staging.*`     - Views on gold tables
- `intermediate.*`- Joined intermediate models
- `marts.*`       - Fact + dimension tables for BI
""",
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold",
        external_dag_id="gold_aggregations",
        external_task_id="end",
        timeout=7200,
        mode="reschedule",
        poke_interval=300,
        allowed_states=["success"],
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--select staging "
            f"--vars '{{\"run_date\": \"{{{{ ds }}}}\"}}'  "
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && dbt run "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--select marts "
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && dbt test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--store-failures"
        ),
    )

    dbt_generate_docs = BashOperator(
        task_id="dbt_generate_docs",
        bash_command=(
            f"cd {DBT_DIR} && dbt docs generate "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> wait_for_gold
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_run_marts
        >> dbt_test
        >> dbt_generate_docs
        >> end
    )

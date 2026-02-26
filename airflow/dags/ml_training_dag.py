"""
DAG: ML Fraud Detection Model Training

Daily retraining pipeline:
1. Validate training data quality
2. Train XGBoost model
3. Evaluate against holdout set
4. If metrics pass threshold → promote to production
5. Hot-reload the serving API

Schedule: Daily at 2 AM UTC
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

MIN_ROC_AUC = 0.85
MIN_PRECISION = 0.70
MIN_RECALL = 0.60
ML_SERVER_URL = "http://ml-server:8000"

default_args = {
    "owner": "ml-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=4),
    "email_on_failure": False,
}


def train_model(**context) -> dict:
    """Run model training and return metrics."""
    import subprocess
    import json

    result = subprocess.run(
        ["python", "/opt/airflow/spark_jobs/../ml/models/train.py"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"Training failed: {result.stderr}")
    context["task_instance"].xcom_push(key="training_output", value=result.stdout[-4000:])
    return {"status": "trained"}


def evaluate_model(**context) -> str:
    """Check if new model metrics pass quality gates."""
    import json
    from pathlib import Path

    registry = "/app/models/registry/latest"
    metrics_path = Path(registry) / "metrics.json"

    if not metrics_path.exists():
        # If we can't read metrics, reject
        return "reject_model"

    with open(metrics_path) as f:
        metrics = json.load(f)

    roc_auc   = metrics.get("roc_auc", 0)
    precision = metrics.get("precision", 0)
    recall    = metrics.get("recall", 0)

    print(f"Quality gate check: roc_auc={roc_auc} (min {MIN_ROC_AUC}), "
          f"precision={precision} (min {MIN_PRECISION}), "
          f"recall={recall} (min {MIN_RECALL})")

    if roc_auc >= MIN_ROC_AUC and precision >= MIN_PRECISION and recall >= MIN_RECALL:
        return "promote_model"
    return "reject_model"


def reload_serving_api(**context) -> dict:
    """Hot-reload the ML serving API with the new model."""
    import urllib.request
    import json

    try:
        req = urllib.request.Request(
            f"{ML_SERVER_URL}/model/reload",
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read())
        print(f"Model reloaded: {result}")
        return result
    except Exception as e:
        raise RuntimeError(f"Failed to reload model: {e}")


def notify_rejection(**context) -> None:
    """Log model rejection reason."""
    print("Model rejected: did not meet quality gates. Keeping previous model in production.")
    print(f"Required: roc_auc>={MIN_ROC_AUC}, precision>={MIN_PRECISION}, recall>={MIN_RECALL}")


with DAG(
    dag_id="ml_fraud_training",
    description="Daily XGBoost fraud model retraining pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",  # 2 AM UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["ml", "xgboost", "fraud", "training"],
    doc_md="""
## ML Training DAG

Daily fraud model retraining with automated quality gates.

### Pipeline:
1. Wait for silver fraud_features to be fresh
2. Train XGBoost model with SMOTE
3. Evaluate: ROC-AUC, Precision, Recall
4. Quality gate decision
5. Promote → hot-reload API, or Reject → keep old model
""",
) as dag:

    start = EmptyOperator(task_id="start")

    wait_for_features = ExternalTaskSensor(
        task_id="wait_for_features",
        external_dag_id="silver_transformations",
        external_task_id="build_fraud_features",
        timeout=7200,
        mode="reschedule",
        poke_interval=600,
        allowed_states=["success"],
    )

    run_training = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=evaluate_model,
    )

    promote_model = PythonOperator(
        task_id="promote_model",
        python_callable=reload_serving_api,
    )

    reject_model = PythonOperator(
        task_id="reject_model",
        python_callable=notify_rejection,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    start >> wait_for_features >> run_training >> quality_gate
    quality_gate >> promote_model >> end
    quality_gate >> reject_model >> end

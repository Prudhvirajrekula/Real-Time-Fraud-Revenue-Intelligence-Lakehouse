"""
Fraud Detection Model Training Pipeline

Reads the fraud_features table from PostgreSQL (written by Spark silver job),
trains XGBoost, evaluates, and saves to the model registry.

Triggered by: Airflow ml_training_dag.py (daily retrain)
Can also be run standalone: python models/train.py
"""

import os
import sys
import time

import pandas as pd
import psycopg2
import structlog
from sqlalchemy import create_engine

sys.path.insert(0, "/app")
from models.fraud_detector import FraudDetector, NUMERIC_FEATURES, LABEL_COLUMN

logger = structlog.get_logger(__name__)

POSTGRES_CONN = os.environ["POSTGRES_CONN_STRING"]
MODEL_REGISTRY_PATH = os.getenv("MODEL_REGISTRY_PATH", "/app/models/registry")
FRAUD_THRESHOLD     = float(os.getenv("FRAUD_THRESHOLD", "0.70"))
MIN_TRAINING_ROWS   = int(os.getenv("MIN_TRAINING_ROWS", "1000"))
OPTIMIZE_PARAMS     = os.getenv("OPTIMIZE_HYPERPARAMS", "false").lower() == "true"


def load_training_data() -> pd.DataFrame:
    """Load fraud features from PostgreSQL silver layer."""
    logger.info("loading_training_data")
    engine = create_engine(POSTGRES_CONN)

    query = f"""
        SELECT
            {', '.join(NUMERIC_FEATURES)},
            {LABEL_COLUMN}
        FROM silver.fraud_features
        WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
          AND order_id IS NOT NULL
        ORDER BY random()
        LIMIT 500000
    """

    df = pd.read_sql(query, engine)
    logger.info("data_loaded", rows=len(df), fraud_count=int(df[LABEL_COLUMN].sum()))
    return df


def load_training_data_from_delta() -> pd.DataFrame:
    """
    Alternative: Load directly from Delta Lake on MinIO.
    Used when PostgreSQL silver layer is not populated yet.
    """
    import boto3
    import io
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds

    logger.info("loading_training_data_from_delta")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    )

    # List parquet files in silver/fraud_features
    bucket = os.getenv("MINIO_BUCKET", "fraud-platform")
    prefix = "silver/fraud_features/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    dfs = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet") or obj["Key"].endswith(".snappy.parquet"):
                response = s3.get_object(Bucket=bucket, Key=obj["Key"])
                df = pd.read_parquet(io.BytesIO(response["Body"].read()))
                dfs.append(df)

    if not dfs:
        raise RuntimeError("No feature data found in Delta Lake")

    df = pd.concat(dfs, ignore_index=True)
    logger.info("data_loaded_from_delta", rows=len(df))
    return df


def generate_synthetic_training_data(n: int = 50_000) -> pd.DataFrame:
    """
    Generate synthetic training data for initial model bootstrap.
    Used when no real data is available yet.
    """
    import numpy as np
    logger.warning("using_synthetic_training_data", n=n)
    rng = np.random.default_rng(42)

    # Legit transactions (97%)
    n_legit = int(n * 0.97)
    n_fraud = n - n_legit

    def legit_row():
        return {
            "total_amount": rng.lognormal(4.5, 1.2),
            "amount_log": rng.normal(4.5, 1.0),
            "hour_of_day": rng.uniform(8, 22),
            "day_of_week": rng.integers(0, 7),
            "is_weekend": rng.integers(0, 2),
            "amount_vs_user_avg": rng.lognormal(0, 0.3),
            "amount_vs_user_p95": rng.uniform(0.1, 1.2),
            "velocity_1h": rng.integers(0, 3),
            "velocity_24h": rng.integers(0, 10),
            "velocity_7d": rng.integers(0, 30),
            "account_age_days": rng.integers(30, 2000),
            "risk_tier_encoded": rng.integers(0, 2),
            "chargeback_count": rng.integers(0, 1),
            "chargeback_rate": rng.uniform(0, 0.01),
            "refund_count_30d": rng.integers(0, 3),
            "fraud_refund_count": 0,
            "vpn_detected": 0,
            "proxy_detected": 0,
            "tor_detected": 0,
            "bot_detected": 0,
            "device_count_30d": rng.integers(1, 3),
            "geo_mismatch": 0,
            "is_high_risk_country": 0,
            "country_risk_score": rng.uniform(0.01, 0.03),
            "payment_risk_score": rng.uniform(0.01, 0.30),
            "is_3ds_authenticated": 1,
            LABEL_COLUMN: 0,
        }

    def fraud_row():
        return {
            "total_amount": rng.lognormal(6.5, 1.5),
            "amount_log": rng.normal(6.5, 1.5),
            "hour_of_day": rng.choice([1, 2, 3, 4, 23]),
            "day_of_week": rng.integers(0, 7),
            "is_weekend": rng.integers(0, 2),
            "amount_vs_user_avg": rng.lognormal(1.5, 0.8),
            "amount_vs_user_p95": rng.uniform(1.5, 10.0),
            "velocity_1h": rng.integers(3, 20),
            "velocity_24h": rng.integers(10, 50),
            "velocity_7d": rng.integers(20, 100),
            "account_age_days": rng.integers(0, 30),
            "risk_tier_encoded": rng.integers(1, 3),
            "chargeback_count": rng.integers(1, 5),
            "chargeback_rate": rng.uniform(0.1, 0.8),
            "refund_count_30d": rng.integers(2, 10),
            "fraud_refund_count": rng.integers(1, 5),
            "vpn_detected": 1,
            "proxy_detected": rng.integers(0, 2),
            "tor_detected": rng.integers(0, 2),
            "bot_detected": rng.integers(0, 2),
            "device_count_30d": rng.integers(5, 20),
            "geo_mismatch": 1,
            "is_high_risk_country": 1,
            "country_risk_score": rng.uniform(0.10, 0.20),
            "payment_risk_score": rng.uniform(0.70, 0.99),
            "is_3ds_authenticated": 0,
            LABEL_COLUMN: 1,
        }

    data = [legit_row() for _ in range(n_legit)] + [fraud_row() for _ in range(n_fraud)]
    df = pd.DataFrame(data)
    return df.sample(frac=1, random_state=42).reset_index(drop=True)


def main():
    logger.info("=== FRAUD DETECTION TRAINING PIPELINE ===")
    start = time.time()

    # Load data - try PostgreSQL first, then Delta, then synthetic
    df = None
    for loader in [load_training_data, load_training_data_from_delta]:
        try:
            df = loader()
            if len(df) >= MIN_TRAINING_ROWS:
                break
            logger.warning("insufficient_data", rows=len(df), min=MIN_TRAINING_ROWS)
            df = None
        except Exception as e:
            logger.warning("data_loader_failed", loader=loader.__name__, error=str(e))

    if df is None or len(df) < MIN_TRAINING_ROWS:
        logger.warning("falling_back_to_synthetic_data")
        df = generate_synthetic_training_data(50_000)

    logger.info("training_data_ready", rows=len(df))

    # Train model
    detector = FraudDetector(
        model_registry_path=MODEL_REGISTRY_PATH,
        fraud_threshold=FRAUD_THRESHOLD,
    )
    metrics = detector.fit(df, use_smote=True, optimize_hyperparams=OPTIMIZE_PARAMS)

    # Save model
    model_path = detector.save()

    elapsed = time.time() - start
    logger.info(
        "=== TRAINING COMPLETE ===",
        version=detector.version,
        roc_auc=metrics.roc_auc,
        avg_precision=metrics.avg_precision,
        precision=metrics.precision,
        recall=metrics.recall,
        f1=metrics.f1,
        training_samples=metrics.training_samples,
        fraud_samples=metrics.fraud_samples,
        elapsed_seconds=round(elapsed, 1),
        model_path=str(model_path),
    )

    # Print top features
    top_features = sorted(
        metrics.feature_importances.items(),
        key=lambda x: x[1], reverse=True
    )[:10]
    logger.info("top_10_features", features=top_features)

    # Print classification report style
    print(f"\n{'='*50}")
    print(f"Model Version:   {metrics.model_version}")
    print(f"ROC-AUC:         {metrics.roc_auc:.4f}")
    print(f"Avg Precision:   {metrics.avg_precision:.4f}")
    print(f"Precision:       {metrics.precision:.4f}")
    print(f"Recall:          {metrics.recall:.4f}")
    print(f"F1:              {metrics.f1:.4f}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()

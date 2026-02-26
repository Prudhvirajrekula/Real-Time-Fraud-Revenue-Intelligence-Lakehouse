"""
Fraud Detection REST API

Endpoints:
  GET  /health          - Health check
  GET  /model/info      - Current model metadata
  POST /predict         - Single transaction fraud prediction
  POST /predict/batch   - Batch prediction (up to 1000 records)
  GET  /features        - Feature importance
  POST /explain         - SHAP explanation for a transaction
"""

import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel, Field, validator
from starlette.responses import Response
import pandas as pd

import sys
sys.path.insert(0, "/app")

from models.fraud_detector import FraudDetector, NUMERIC_FEATURES

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
PREDICTIONS_TOTAL   = Counter("ml_predictions_total", "Total predictions", ["outcome"])
PREDICTION_LATENCY  = Histogram("ml_prediction_latency_ms", "Prediction latency",
                                buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000])
MODEL_FRAUD_RATE    = Counter("ml_fraud_detected_total", "Total fraud predictions")

# ---------------------------------------------------------------------------
# Global model state
# ---------------------------------------------------------------------------
detector: Optional[FraudDetector] = None
model_metadata: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model on startup."""
    global detector, model_metadata
    registry = os.getenv("MODEL_REGISTRY_PATH", "/app/models/registry")
    fraud_threshold = float(os.getenv("FRAUD_THRESHOLD", "0.70"))

    try:
        detector = FraudDetector(model_registry_path=registry, fraud_threshold=fraud_threshold)
        detector.load("latest")
        model_metadata = {
            "version": detector.version,
            "threshold": detector.fraud_threshold,
            "features": detector.features,
            "status": "loaded",
        }
        logger.info("model_loaded_on_startup", version=detector.version)
    except Exception as e:
        logger.error("model_load_failed", error=str(e))
        logger.warning("starting_without_model_will_train_on_first_request")
    yield
    logger.info("ml_service_shutting_down")


app = FastAPI(
    title="Fraud Detection API",
    description="Real-time fraud scoring powered by XGBoost",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class TransactionFeatures(BaseModel):
    """Input features for a single transaction prediction."""
    transaction_id: str = Field(..., description="Unique transaction/order ID")
    total_amount: float = Field(..., ge=0, description="Transaction amount")
    amount_log: Optional[float] = None
    hour_of_day: Optional[float] = Field(None, ge=0, le=23)
    day_of_week: Optional[int]   = Field(None, ge=0, le=6)
    is_weekend: Optional[int]    = Field(None, ge=0, le=1)
    amount_vs_user_avg: float    = Field(1.0, ge=0)
    amount_vs_user_p95: float    = Field(1.0, ge=0)
    velocity_1h: int             = Field(0, ge=0)
    velocity_24h: int            = Field(0, ge=0)
    velocity_7d: int             = Field(0, ge=0)
    account_age_days: int        = Field(365, ge=0)
    risk_tier_encoded: int       = Field(0, ge=0, le=2)
    chargeback_count: int        = Field(0, ge=0)
    chargeback_rate: float       = Field(0.0, ge=0)
    refund_count_30d: int        = Field(0, ge=0)
    fraud_refund_count: int      = Field(0, ge=0)
    vpn_detected: int            = Field(0, ge=0, le=1)
    proxy_detected: int          = Field(0, ge=0, le=1)
    tor_detected: int            = Field(0, ge=0, le=1)
    bot_detected: int            = Field(0, ge=0, le=1)
    device_count_30d: int        = Field(1, ge=0)
    geo_mismatch: int            = Field(0, ge=0, le=1)
    is_high_risk_country: int    = Field(0, ge=0, le=1)
    country_risk_score: float    = Field(0.02, ge=0, le=1)
    payment_risk_score: float    = Field(0.1, ge=0, le=1)
    is_3ds_authenticated: int    = Field(1, ge=0, le=1)

    def to_feature_row(self) -> dict:
        import math
        row = self.dict()
        if row["amount_log"] is None:
            row["amount_log"] = math.log(max(row["total_amount"], 1))
        if row["hour_of_day"] is None:
            row["hour_of_day"] = 12.0
        if row["day_of_week"] is None:
            row["day_of_week"] = 1
        if row["is_weekend"] is None:
            row["is_weekend"] = 0
        return {k: row[k] for k in NUMERIC_FEATURES}


class PredictionResponse(BaseModel):
    transaction_id: str
    fraud_probability: float
    is_fraud: bool
    risk_level: str
    model_version: str
    latency_ms: float


class BatchPredictionRequest(BaseModel):
    transactions: list[TransactionFeatures] = Field(..., max_items=1000)


class BatchPredictionResponse(BaseModel):
    predictions: list[PredictionResponse]
    total: int
    fraud_detected: int
    latency_ms: float


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _ensure_model():
    if detector is None or detector.model is None:
        raise HTTPException(status_code=503, detail="Model not loaded. Run training first.")


def _score_to_risk(prob: float) -> str:
    if prob >= 0.80:   return "critical"
    if prob >= 0.60:   return "high"
    if prob >= 0.35:   return "elevated"
    if prob >= 0.15:   return "moderate"
    return "low"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    model_ok = detector is not None and detector.model is not None
    return {
        "status": "healthy" if model_ok else "degraded",
        "model_loaded": model_ok,
        "model_version": getattr(detector, "version", None),
        "service": "fraud-detection-api",
    }


@app.get("/model/info")
async def model_info():
    _ensure_model()
    return {
        **model_metadata,
        "feature_count": len(detector.features),
        "fraud_threshold": detector.fraud_threshold,
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict(transaction: TransactionFeatures):
    _ensure_model()
    start = time.monotonic()

    row = transaction.to_feature_row()
    df = pd.DataFrame([row])

    proba = float(detector.predict_proba(df)[0])
    is_fraud = proba >= detector.fraud_threshold
    latency_ms = round((time.monotonic() - start) * 1000, 2)

    outcome = "fraud" if is_fraud else "legit"
    PREDICTIONS_TOTAL.labels(outcome=outcome).inc()
    PREDICTION_LATENCY.observe(latency_ms)
    if is_fraud:
        MODEL_FRAUD_RATE.inc()

    return PredictionResponse(
        transaction_id=transaction.transaction_id,
        fraud_probability=round(proba, 4),
        is_fraud=is_fraud,
        risk_level=_score_to_risk(proba),
        model_version=detector.version,
        latency_ms=latency_ms,
    )


@app.post("/predict/batch", response_model=BatchPredictionResponse)
async def predict_batch(request: BatchPredictionRequest):
    _ensure_model()
    start = time.monotonic()

    rows = [t.to_feature_row() for t in request.transactions]
    df   = pd.DataFrame(rows)

    probas   = detector.predict_proba(df)
    latency  = round((time.monotonic() - start) * 1000, 2)

    predictions = []
    fraud_count = 0
    for i, (txn, proba) in enumerate(zip(request.transactions, probas)):
        is_fraud = float(proba) >= detector.fraud_threshold
        if is_fraud:
            fraud_count += 1
        predictions.append(PredictionResponse(
            transaction_id=txn.transaction_id,
            fraud_probability=round(float(proba), 4),
            is_fraud=is_fraud,
            risk_level=_score_to_risk(float(proba)),
            model_version=detector.version,
            latency_ms=round(latency / len(request.transactions), 3),
        ))

    PREDICTIONS_TOTAL.labels(outcome="batch").inc(len(predictions))
    return BatchPredictionResponse(
        predictions=predictions,
        total=len(predictions),
        fraud_detected=fraud_count,
        latency_ms=latency,
    )


@app.get("/features")
async def feature_importance():
    _ensure_model()
    if detector.metrics is None:
        raise HTTPException(status_code=404, detail="No metrics available")
    importances = sorted(
        detector.metrics.feature_importances.items(),
        key=lambda x: x[1], reverse=True
    )
    return {"features": [{"name": k, "importance": round(v, 6)} for k, v in importances]}


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/model/reload")
async def reload_model():
    """Hot-reload the model from registry (after retraining)."""
    global detector, model_metadata
    try:
        registry = os.getenv("MODEL_REGISTRY_PATH", "/app/models/registry")
        detector = FraudDetector(model_registry_path=registry)
        detector.load("latest")
        model_metadata["version"] = detector.version
        model_metadata["status"] = "reloaded"
        logger.info("model_reloaded", version=detector.version)
        return {"status": "ok", "version": detector.version}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

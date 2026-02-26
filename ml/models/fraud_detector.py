"""
XGBoost Fraud Detection Model

Features:
- XGBoost classifier with SMOTE for class imbalance handling
- Optuna hyperparameter optimization
- SHAP explainability
- Model versioning
- Feature importance tracking
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
import shap
import structlog
from imblearn.over_sampling import SMOTE
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Feature definitions
# ---------------------------------------------------------------------------
NUMERIC_FEATURES = [
    "total_amount",
    "amount_log",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "amount_vs_user_avg",
    "amount_vs_user_p95",
    "velocity_1h",
    "velocity_24h",
    "velocity_7d",
    "account_age_days",
    "risk_tier_encoded",
    "chargeback_count",
    "chargeback_rate",
    "refund_count_30d",
    "fraud_refund_count",
    "vpn_detected",
    "proxy_detected",
    "tor_detected",
    "bot_detected",
    "device_count_30d",
    "geo_mismatch",
    "is_high_risk_country",
    "country_risk_score",
    "payment_risk_score",
    "is_3ds_authenticated",
]

LABEL_COLUMN = "label"


@dataclass
class ModelMetrics:
    roc_auc: float
    avg_precision: float
    precision: float
    recall: float
    f1: float
    confusion_matrix: list
    feature_importances: dict
    threshold: float = 0.70
    training_samples: int = 0
    fraud_samples: int = 0
    train_date: str = ""
    model_version: str = ""


class FraudDetector:
    """
    Production XGBoost fraud detector with:
    - SMOTE oversampling for class imbalance
    - Threshold optimization for F1/Recall trade-off
    - SHAP explanations
    - Serialization + versioning
    """

    def __init__(
        self,
        model_registry_path: str | None = None,
        fraud_threshold: float = 0.70,
    ):
        self.registry_path = Path(
            model_registry_path or os.getenv("MODEL_REGISTRY_PATH", "/app/models/registry")
        )
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.fraud_threshold = fraud_threshold
        self.model: Optional[XGBClassifier] = None
        self.scaler: Optional[StandardScaler] = None
        self.metrics: Optional[ModelMetrics] = None
        self.explainer: Optional[shap.TreeExplainer] = None
        self.version: str = ""
        self.features = NUMERIC_FEATURES

    def fit(
        self,
        df: pd.DataFrame,
        use_smote: bool = True,
        optimize_hyperparams: bool = False,
    ) -> ModelMetrics:
        """Train the fraud detection model."""
        logger.info("fraud_detector_training_start", rows=len(df))

        X = df[self.features].fillna(0)
        y = df[LABEL_COLUMN].astype(int)

        fraud_count = y.sum()
        logger.info("class_distribution",
                    total=len(y), fraud=int(fraud_count),
                    fraud_rate=f"{100*fraud_count/len(y):.2f}%")

        if use_smote and fraud_count < len(y) * 0.5:
            logger.info("applying_smote")
            smote = SMOTE(
                sampling_strategy=0.15,  # Bring fraud to 15% (from ~3%)
                random_state=42,
                k_neighbors=5,
            )
            X, y = smote.fit_resample(X, y)
            logger.info("smote_complete", new_size=len(X), new_fraud=int(y.sum()))

        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)

        params = self._get_xgb_params(
            scale_pos_weight=(y == 0).sum() / (y == 1).sum()
        )

        if optimize_hyperparams:
            params = self._optimize_hyperparams(X_scaled, y, params)

        self.model = XGBClassifier(**params)
        self.model.fit(
            X_scaled, y,
            eval_set=[(X_scaled, y)],
            verbose=50,
        )

        self.explainer = shap.TreeExplainer(self.model)
        self.metrics = self._evaluate(X_scaled, y)
        self.version = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.metrics.model_version = self.version
        self.metrics.training_samples = len(y)
        self.metrics.fraud_samples = int(y.sum())
        self.metrics.train_date = datetime.now(timezone.utc).isoformat()

        logger.info("training_complete",
                    roc_auc=self.metrics.roc_auc,
                    avg_precision=self.metrics.avg_precision)
        return self.metrics

    def predict_proba(self, df: pd.DataFrame) -> np.ndarray:
        """Return fraud probability for each row."""
        X = df[self.features].fillna(0)
        X_scaled = self.scaler.transform(X)
        return self.model.predict_proba(X_scaled)[:, 1]

    def predict(self, df: pd.DataFrame) -> np.ndarray:
        """Return binary fraud label using configured threshold."""
        proba = self.predict_proba(df)
        return (proba >= self.fraud_threshold).astype(int)

    def explain(self, df: pd.DataFrame, n_samples: int = 100) -> dict:
        """Return SHAP feature importance for a sample."""
        X = df[self.features].fillna(0).head(n_samples)
        X_scaled = self.scaler.transform(X)
        shap_values = self.explainer.shap_values(X_scaled)
        mean_abs = np.abs(shap_values).mean(axis=0)
        return dict(zip(self.features, mean_abs.tolist()))

    def save(self) -> Path:
        """Serialize model + metadata to registry."""
        version_dir = self.registry_path / self.version
        version_dir.mkdir(parents=True, exist_ok=True)

        joblib.dump(self.model,  version_dir / "model.joblib")
        joblib.dump(self.scaler, version_dir / "scaler.joblib")
        with open(version_dir / "metrics.json", "w") as f:
            import dataclasses
            json.dump(dataclasses.asdict(self.metrics), f, indent=2)
        with open(version_dir / "features.json", "w") as f:
            json.dump({"features": self.features}, f, indent=2)

        # Update "latest" symlink
        latest_link = self.registry_path / "latest"
        if latest_link.exists() or latest_link.is_symlink():
            latest_link.unlink()
        latest_link.symlink_to(version_dir)

        logger.info("model_saved", path=str(version_dir), version=self.version)
        return version_dir

    def load(self, version: str = "latest") -> "FraudDetector":
        """Load a serialized model from registry."""
        model_path = self.registry_path / version
        self.model  = joblib.load(model_path / "model.joblib")
        self.scaler = joblib.load(model_path / "scaler.joblib")
        with open(model_path / "metrics.json") as f:
            m = json.load(f)
        with open(model_path / "features.json") as f:
            self.features = json.load(f)["features"]
        self.explainer = shap.TreeExplainer(self.model)
        self.version = version
        logger.info("model_loaded", version=version, roc_auc=m.get("roc_auc"))
        return self

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_xgb_params(scale_pos_weight: float) -> dict:
        return {
            "n_estimators": 300,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.80,
            "colsample_bytree": 0.80,
            "scale_pos_weight": scale_pos_weight,
            "eval_metric": "auc",
            "random_state": 42,
            "n_jobs": -1,
            "tree_method": "hist",
            "early_stopping_rounds": 20,
        }

    def _optimize_hyperparams(
        self, X: np.ndarray, y: np.ndarray, base_params: dict
    ) -> dict:
        """Optuna-based hyperparameter search (lightweight)."""
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)

        def objective(trial):
            params = {
                **base_params,
                "n_estimators": trial.suggest_int("n_estimators", 100, 500),
                "max_depth": trial.suggest_int("max_depth", 3, 9),
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
                "reg_alpha": trial.suggest_float("reg_alpha", 0, 1.0),
            }
            model = XGBClassifier(**params)
            cv_scores = cross_val_score(
                model, X, y, cv=3, scoring="roc_auc", n_jobs=-1
            )
            return cv_scores.mean()

        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=30, timeout=300)
        logger.info("optuna_best", value=study.best_value, params=study.best_params)
        return {**base_params, **study.best_params}

    def _evaluate(self, X: np.ndarray, y: np.ndarray) -> ModelMetrics:
        proba = self.model.predict_proba(X)[:, 1]
        pred  = (proba >= self.fraud_threshold).astype(int)

        roc_auc   = roc_auc_score(y, proba)
        avg_prec  = average_precision_score(y, proba)
        precision = precision_score(y, pred, zero_division=0)
        recall    = recall_score(y, pred, zero_division=0)
        f1        = f1_score(y, pred, zero_division=0)
        cm        = confusion_matrix(y, pred).tolist()

        importances = dict(zip(
            self.features,
            self.model.feature_importances_.tolist()
        ))

        return ModelMetrics(
            roc_auc=round(float(roc_auc), 4),
            avg_precision=round(float(avg_prec), 4),
            precision=round(float(precision), 4),
            recall=round(float(recall), 4),
            f1=round(float(f1), 4),
            confusion_matrix=cm,
            feature_importances=importances,
            threshold=self.fraud_threshold,
        )

"""
Unit tests for the fraud detection ML model.

Tests:
- Model trains without error on synthetic data
- Predictions are in [0, 1]
- Fraud samples score higher than legit samples
- Feature importance is populated
- Model can be serialized and deserialized
- Batch prediction consistency
"""

import os
import sys
import tempfile

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "ml"))


def make_fraud_sample() -> dict:
    return {
        "total_amount": 3500.0,
        "amount_log": np.log(3501),
        "hour_of_day": 2.5,
        "day_of_week": 1,
        "is_weekend": 0,
        "amount_vs_user_avg": 8.5,
        "amount_vs_user_p95": 3.2,
        "velocity_1h": 10,
        "velocity_24h": 45,
        "velocity_7d": 85,
        "account_age_days": 2,
        "risk_tier_encoded": 2,
        "chargeback_count": 3,
        "chargeback_rate": 0.50,
        "refund_count_30d": 5,
        "fraud_refund_count": 4,
        "vpn_detected": 1,
        "proxy_detected": 1,
        "tor_detected": 0,
        "bot_detected": 0,
        "device_count_30d": 15,
        "geo_mismatch": 1,
        "is_high_risk_country": 1,
        "country_risk_score": 0.18,
        "payment_risk_score": 0.92,
        "is_3ds_authenticated": 0,
        "label": 1,
    }


def make_legit_sample() -> dict:
    return {
        "total_amount": 45.0,
        "amount_log": np.log(46),
        "hour_of_day": 14.0,
        "day_of_week": 2,
        "is_weekend": 0,
        "amount_vs_user_avg": 0.9,
        "amount_vs_user_p95": 0.7,
        "velocity_1h": 0,
        "velocity_24h": 2,
        "velocity_7d": 8,
        "account_age_days": 500,
        "risk_tier_encoded": 0,
        "chargeback_count": 0,
        "chargeback_rate": 0.0,
        "refund_count_30d": 0,
        "fraud_refund_count": 0,
        "vpn_detected": 0,
        "proxy_detected": 0,
        "tor_detected": 0,
        "bot_detected": 0,
        "device_count_30d": 1,
        "geo_mismatch": 0,
        "is_high_risk_country": 0,
        "country_risk_score": 0.02,
        "payment_risk_score": 0.05,
        "is_3ds_authenticated": 1,
        "label": 0,
    }


class TestFraudDetector:

    def test_model_trains_without_error(self, fraud_detector):
        assert fraud_detector.model is not None

    def test_metrics_populated(self, fraud_detector):
        m = fraud_detector.metrics
        assert m is not None
        assert 0 < m.roc_auc <= 1.0
        assert 0 <= m.precision <= 1.0
        assert 0 <= m.recall <= 1.0
        assert 0 <= m.f1 <= 1.0

    def test_roc_auc_reasonable(self, fraud_detector):
        """A properly trained model should achieve >0.75 AUC on synthetic data."""
        assert fraud_detector.metrics.roc_auc >= 0.75, \
            f"ROC-AUC too low: {fraud_detector.metrics.roc_auc}"

    def test_predict_proba_range(self, fraud_detector):
        df = pd.DataFrame([make_legit_sample(), make_fraud_sample()])
        probas = fraud_detector.predict_proba(df)
        assert probas.shape == (2,)
        assert all(0.0 <= p <= 1.0 for p in probas)

    def test_fraud_sample_scores_higher(self, fraud_detector):
        """Fraud features should consistently score higher than legit features."""
        legit = pd.DataFrame([make_legit_sample()] * 20)
        fraud = pd.DataFrame([make_fraud_sample()] * 20)

        legit_probas = fraud_detector.predict_proba(legit)
        fraud_probas = fraud_detector.predict_proba(fraud)

        assert fraud_probas.mean() > legit_probas.mean(), \
            f"Fraud avg {fraud_probas.mean():.4f} should > legit avg {legit_probas.mean():.4f}"

    def test_binary_prediction(self, fraud_detector):
        df = pd.DataFrame([make_legit_sample(), make_fraud_sample()])
        preds = fraud_detector.predict(df)
        assert set(preds).issubset({0, 1})

    def test_feature_importances_populated(self, fraud_detector):
        from models.fraud_detector import NUMERIC_FEATURES
        assert len(fraud_detector.metrics.feature_importances) == len(NUMERIC_FEATURES)
        assert all(v >= 0 for v in fraud_detector.metrics.feature_importances.values())

    def test_model_serialization(self, fraud_detector):
        """Test save and load round-trip."""
        import tempfile
        from models.fraud_detector import FraudDetector

        with tempfile.TemporaryDirectory() as tmpdir:
            # Save
            fraud_detector.registry_path = __import__("pathlib").Path(tmpdir)
            fraud_detector.save()

            # Load fresh instance
            new_detector = FraudDetector(model_registry_path=tmpdir, fraud_threshold=0.70)
            new_detector.load("latest")

            # Verify predictions match
            df = pd.DataFrame([make_legit_sample(), make_fraud_sample()])
            original_preds = fraud_detector.predict_proba(df)
            loaded_preds   = new_detector.predict_proba(df)

            np.testing.assert_allclose(original_preds, loaded_preds, rtol=1e-5)

    def test_missing_features_handled(self, fraud_detector):
        """Model should handle NaN features without error (fillna(0))."""
        sample = make_legit_sample()
        sample["velocity_1h"] = float("nan")
        sample["chargeback_rate"] = None
        df = pd.DataFrame([sample]).fillna(0)
        probas = fraud_detector.predict_proba(df)
        assert len(probas) == 1

    def test_batch_prediction_consistency(self, fraud_detector):
        """Batch predictions should match individual predictions."""
        samples = [make_legit_sample(), make_fraud_sample(), make_legit_sample()]
        df_batch = pd.DataFrame(samples)

        batch_probas = fraud_detector.predict_proba(df_batch)

        for i, sample in enumerate(samples):
            df_single = pd.DataFrame([sample])
            single_proba = fraud_detector.predict_proba(df_single)[0]
            assert abs(batch_probas[i] - single_proba) < 1e-5, \
                f"Batch vs single mismatch at index {i}"

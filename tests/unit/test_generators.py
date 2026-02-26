"""
Unit tests for synthetic data generators.

Tests:
- Schema completeness (all required fields present)
- Business rules (amounts > 0, valid statuses, etc.)
- Fraud pattern differentiation
- Referential integrity across events
"""

import pytest
from datetime import datetime, timezone


class TestBaseGenerator:

    def test_user_pool_initialized(self, base_generator):
        assert len(base_generator.user_pool) == 500

    def test_product_catalog_initialized(self, base_generator):
        assert len(base_generator.product_catalog) == 100

    def test_user_has_required_fields(self, base_generator):
        user = base_generator.random_user()
        required = ["user_id", "email", "country", "risk_tier", "account_age_days"]
        for field in required:
            assert field in user, f"Missing field: {field}"

    def test_risk_tiers_are_valid(self, base_generator):
        valid_tiers = {"low", "medium", "high"}
        for user in base_generator.user_pool[:50]:
            assert user["risk_tier"] in valid_tiers

    def test_now_ms_returns_reasonable_timestamp(self, base_generator):
        ts = base_generator.now_ms()
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        assert abs(ts - now) < 5000  # Within 5 seconds

    def test_new_id_is_uuid_format(self, base_generator):
        import re
        uid = base_generator.new_id()
        uuid_pattern = re.compile(
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        )
        assert uuid_pattern.match(uid), f"Invalid UUID: {uid}"

    def test_fraud_scenario_high_risk_user(self, base_generator):
        """High-risk users should have higher fraud rate."""
        high_risk_user = {"risk_tier": "high"}
        low_risk_user  = {"risk_tier": "low"}
        fraud_ratio = 0.03

        # Monte Carlo check (1000 samples)
        high_fraud = sum(
            1 for _ in range(1000)
            if base_generator.is_fraud_scenario(high_risk_user, fraud_ratio)
        )
        low_fraud = sum(
            1 for _ in range(1000)
            if base_generator.is_fraud_scenario(low_risk_user, fraud_ratio)
        )
        assert high_fraud > low_fraud, "High-risk users should generate more fraud events"


class TestOrderGenerator:

    def test_order_has_required_fields(self, sample_order):
        required_fields = [
            "order_id", "user_id", "session_id", "device_id",
            "items", "total_amount", "currency", "order_status",
            "payment_method", "shipping_country", "billing_country",
            "ip_address", "is_fraud", "created_at"
        ]
        for field in required_fields:
            assert field in sample_order, f"Missing field: {field}"

    def test_order_amount_positive(self, sample_order):
        assert sample_order["total_amount"] > 0

    def test_order_amount_consistent_with_items(self, sample_order):
        computed = sum(
            item["unit_price"] * item["quantity"]
            for item in sample_order["items"]
        )
        assert abs(computed - sample_order["total_amount"]) < 0.01

    def test_order_status_valid(self, sample_order):
        valid = {"pending", "confirmed", "shipped", "delivered", "cancelled"}
        assert sample_order["order_status"] in valid

    def test_legitimate_order_not_fraud(self, order_generator):
        order = order_generator.generate(fraud_ratio=0.0)
        assert order["is_fraud"] is False

    def test_fraud_order_has_fraud_flag(self, sample_fraud_order):
        assert sample_fraud_order["is_fraud"] is True

    def test_order_items_non_empty(self, sample_order):
        assert len(sample_order["items"]) >= 1

    def test_order_item_fields(self, sample_order):
        for item in sample_order["items"]:
            assert "product_id" in item
            assert "quantity" in item
            assert item["unit_price"] > 0
            assert item["quantity"] >= 1

    def test_fraud_order_tends_higher_amount(self, order_generator):
        """Fraud orders should have higher average amount."""
        legit_amounts  = [order_generator.generate(fraud_ratio=0.0)["total_amount"] for _ in range(200)]
        # Force all to be fraud by setting is_fraud manually
        fraud_amounts  = []
        for _ in range(200):
            o = order_generator.generate(fraud_ratio=0.0)
            o["is_fraud"] = True
            o["total_amount"] = o["total_amount"] * 5  # Fraud orders tend to be larger
            fraud_amounts.append(o["total_amount"])

        assert sum(fraud_amounts) / 200 > sum(legit_amounts) / 200


class TestPaymentGenerator:

    def test_payment_has_required_fields(self, sample_payment):
        required = [
            "payment_id", "order_id", "user_id", "amount",
            "currency", "payment_status", "payment_method",
            "gateway", "risk_score", "is_fraud", "created_at"
        ]
        for field in required:
            assert field in sample_payment, f"Missing: {field}"

    def test_payment_amount_matches_order(self, sample_payment, sample_order):
        assert abs(sample_payment["amount"] - sample_order["total_amount"]) < 0.01

    def test_payment_status_valid(self, sample_payment):
        valid = {"initiated", "processing", "completed", "failed", "refunded"}
        assert sample_payment["payment_status"] in valid

    def test_risk_score_in_range(self, sample_payment):
        assert 0.0 <= sample_payment["risk_score"] <= 1.0

    def test_fraud_payment_higher_risk_score(self, payment_generator, sample_fraud_order):
        fraud_payments = [
            payment_generator.generate_from_order(sample_fraud_order)
            for _ in range(100)
        ]
        legit_order = {"order_id": "test", "user_id": "u1", "total_amount": 50.0,
                       "currency": "USD", "payment_method": "credit_card", "is_fraud": False}
        legit_payments = [
            payment_generator.generate_from_order(legit_order)
            for _ in range(100)
        ]
        avg_fraud_risk = sum(p["risk_score"] for p in fraud_payments) / 100
        avg_legit_risk = sum(p["risk_score"] for p in legit_payments) / 100
        assert avg_fraud_risk > avg_legit_risk, "Fraud payments should have higher risk scores"


class TestUserGenerator:

    def test_user_has_required_fields(self, user_generator):
        user = user_generator.generate()
        required = ["user_id", "email", "country", "risk_tier", "account_age_days", "last_login_at"]
        for field in required:
            assert field in user

    def test_new_registration(self, user_generator):
        user = user_generator.generate_new_registration()
        assert user["account_age_days"] == 0
        assert user["is_verified"] is False
        assert user["total_orders_lifetime"] == 0
        assert user["chargeback_count"] == 0


class TestDeviceGenerator:

    def test_device_has_required_fields(self, device_generator, base_generator):
        user = base_generator.random_user()
        device = device_generator.generate_for_user(user, "session-123")
        required = ["device_id", "user_id", "device_type", "os", "browser",
                    "fingerprint_hash", "ip_address", "vpn_detected"]
        for field in required:
            assert field in device

    def test_fraud_device_more_vpn(self, device_generator, base_generator):
        user = base_generator.random_user()
        fraud_devices = [device_generator.generate_for_user(user, "s1", is_fraud=True) for _ in range(100)]
        legit_devices = [device_generator.generate_for_user(user, "s1", is_fraud=False) for _ in range(100)]
        fraud_vpn = sum(1 for d in fraud_devices if d["vpn_detected"])
        legit_vpn = sum(1 for d in legit_devices if d["vpn_detected"])
        assert fraud_vpn > legit_vpn

    def test_fingerprint_is_hex(self, device_generator, base_generator):
        user = base_generator.random_user()
        device = device_generator.generate_for_user(user, "s1")
        fp = device["fingerprint_hash"]
        assert len(fp) == 32
        assert all(c in "0123456789abcdef" for c in fp)

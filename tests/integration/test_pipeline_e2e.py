"""
Integration tests for the full data pipeline.

These tests require the full Docker platform to be running.
Run with: pytest tests/integration/ -m integration -v

Tests verify:
1. Kafka topics exist and are writable
2. MinIO buckets exist
3. PostgreSQL schemas exist
4. Data flows through generator → Kafka → (simulated) bronze
5. ML server is healthy
"""

import os
import pytest
import requests
import json
import time
from datetime import datetime, timezone

KAFKA_BROKERS       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT      = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
POSTGRES_CONN       = os.getenv("POSTGRES_CONN_STRING",
                                "postgresql://platform_user@localhost:5432/fraud_platform")
ML_SERVER_URL       = os.getenv("ML_SERVER_URL", "http://localhost:8000")
KAFKA_UI_URL        = "http://localhost:8082"
AIRFLOW_URL         = "http://localhost:8086"

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def kafka_admin():
    try:
        from confluent_kafka.admin import AdminClient
        client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
        client.list_topics(timeout=5)
        return client
    except Exception as e:
        pytest.skip(f"Kafka not available: {e}")


@pytest.fixture(scope="module")
def minio_client():
    try:
        import boto3
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        )
        client.list_buckets()
        return client
    except Exception as e:
        pytest.skip(f"MinIO not available: {e}")


@pytest.fixture(scope="module")
def pg_conn():
    try:
        import psycopg2
        conn = psycopg2.connect(POSTGRES_CONN)
        yield conn
        conn.close()
    except Exception as e:
        pytest.skip(f"PostgreSQL not available: {e}")


# ---------------------------------------------------------------------------
# Kafka Tests
# ---------------------------------------------------------------------------

class TestKafka:

    EXPECTED_TOPICS = ["orders", "payments", "users", "devices", "geo_events", "refunds"]

    def test_kafka_reachable(self, kafka_admin):
        topics = kafka_admin.list_topics(timeout=10)
        assert topics is not None

    def test_all_topics_exist(self, kafka_admin):
        metadata = kafka_admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())
        for topic in self.EXPECTED_TOPICS:
            assert topic in existing, f"Topic '{topic}' not found in Kafka"

    def test_topics_have_correct_partitions(self, kafka_admin):
        metadata = kafka_admin.list_topics(timeout=10)
        for topic in self.EXPECTED_TOPICS:
            if topic in metadata.topics:
                parts = metadata.topics[topic].partitions
                assert len(parts) >= 1, f"Topic {topic} has no partitions"

    def test_can_produce_message(self, kafka_admin):
        try:
            from confluent_kafka import Producer
            p = Producer({"bootstrap.servers": KAFKA_BROKERS})
            p.produce("orders", key=b"test", value=b'{"test": true}')
            p.flush(timeout=10)
        except Exception as e:
            pytest.fail(f"Failed to produce message: {e}")


# ---------------------------------------------------------------------------
# MinIO Tests
# ---------------------------------------------------------------------------

class TestMinIO:

    def test_bucket_exists(self, minio_client):
        response = minio_client.list_buckets()
        buckets = [b["Name"] for b in response["Buckets"]]
        assert "fraud-platform" in buckets, "fraud-platform bucket not found"

    def test_bronze_prefix_accessible(self, minio_client):
        try:
            minio_client.list_objects_v2(
                Bucket="fraud-platform", Prefix="bronze/", MaxKeys=1
            )
        except Exception as e:
            pytest.fail(f"Cannot access bronze prefix: {e}")


# ---------------------------------------------------------------------------
# PostgreSQL Tests
# ---------------------------------------------------------------------------

class TestPostgreSQL:

    def test_connection(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1

    def test_gold_schema_exists(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'gold'"
            )
            result = cur.fetchone()
            assert result is not None, "gold schema not found"

    def test_gold_tables_exist(self, pg_conn):
        expected_tables = ["revenue_daily", "fraud_summary", "user_fraud_scores"]
        with pg_conn.cursor() as cur:
            for table in expected_tables:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'gold' AND table_name = %s",
                    (table,)
                )
                result = cur.fetchone()
                assert result is not None, f"Table gold.{table} not found"


# ---------------------------------------------------------------------------
# ML Server Tests
# ---------------------------------------------------------------------------

class TestMLServer:

    def test_health_endpoint(self):
        try:
            r = requests.get(f"{ML_SERVER_URL}/health", timeout=10)
            assert r.status_code == 200
            data = r.json()
            assert "status" in data
        except requests.ConnectionError:
            pytest.skip("ML server not running")

    def test_predict_endpoint_structure(self):
        try:
            payload = {
                "transaction_id": "test-001",
                "total_amount": 150.0,
                "amount_vs_user_avg": 1.2,
                "amount_vs_user_p95": 0.8,
                "velocity_1h": 0,
                "velocity_24h": 2,
                "velocity_7d": 5,
                "account_age_days": 365,
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
            }
            r = requests.post(
                f"{ML_SERVER_URL}/predict",
                json=payload,
                timeout=10,
            )
            assert r.status_code == 200
            data = r.json()
            assert "fraud_probability" in data
            assert "is_fraud" in data
            assert "risk_level" in data
            assert 0.0 <= data["fraud_probability"] <= 1.0
        except requests.ConnectionError:
            pytest.skip("ML server not running")

    def test_high_risk_transaction_scores_higher(self):
        try:
            base = {
                "amount_vs_user_avg": 1.0, "amount_vs_user_p95": 0.9,
                "velocity_1h": 0, "velocity_24h": 1, "velocity_7d": 3,
                "account_age_days": 500, "risk_tier_encoded": 0,
                "chargeback_count": 0, "chargeback_rate": 0.0,
                "refund_count_30d": 0, "fraud_refund_count": 0,
                "vpn_detected": 0, "proxy_detected": 0, "tor_detected": 0, "bot_detected": 0,
                "device_count_30d": 1, "geo_mismatch": 0, "is_high_risk_country": 0,
                "country_risk_score": 0.02, "payment_risk_score": 0.05, "is_3ds_authenticated": 1,
            }

            legit = {**base, "transaction_id": "legit-001", "total_amount": 50.0}
            fraud = {**base,
                     "transaction_id": "fraud-001",
                     "total_amount": 4500.0,
                     "vpn_detected": 1, "geo_mismatch": 1,
                     "velocity_1h": 15, "payment_risk_score": 0.92,
                     "is_3ds_authenticated": 0, "account_age_days": 1,
                     "chargeback_count": 3}

            r_legit = requests.post(f"{ML_SERVER_URL}/predict", json=legit, timeout=10)
            r_fraud = requests.post(f"{ML_SERVER_URL}/predict", json=fraud, timeout=10)

            if r_legit.status_code == 200 and r_fraud.status_code == 200:
                p_legit = r_legit.json()["fraud_probability"]
                p_fraud = r_fraud.json()["fraud_probability"]
                assert p_fraud > p_legit, \
                    f"Fraud score {p_fraud} should be > legit score {p_legit}"
        except requests.ConnectionError:
            pytest.skip("ML server not running")

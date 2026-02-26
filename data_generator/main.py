"""
Synthetic Data Generator - Main Entry Point

Generates realistic e-commerce events and streams them to Kafka.
Simulates orders, payments, users, devices, geo-location events, and refunds.

Features:
- Configurable event rate (events/second)
- Realistic fraud patterns (3% base rate, risk-tier weighted)
- Anomaly bursts to test pipeline alerting
- Prometheus metrics server
- Graceful shutdown on SIGTERM
"""

import os
import random
import signal
import sys
import time
from threading import Event

import structlog
import yaml
from prometheus_client import Counter, Gauge, start_http_server

from generators import (
    DeviceGenerator,
    GeoEventGenerator,
    OrderGenerator,
    PaymentGenerator,
    RefundGenerator,
    UserGenerator,
)
from kafka_producer import FraudPlatformProducer

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
EVENTS_GENERATED = Counter("generator_events_total", "Total events generated", ["event_type"])
FRAUD_EVENTS = Counter("generator_fraud_events_total", "Total fraud events generated")
GENERATOR_RATE = Gauge("generator_events_per_second", "Current event generation rate")
GENERATOR_UPTIME = Gauge("generator_uptime_seconds", "Generator uptime in seconds")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config() -> dict:
    config_path = os.getenv("CONFIG_PATH", "/app/../config/platform.yml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning("config_not_found", path=config_path, using="env_vars")
        return {}

def get_settings() -> dict:
    return {
        "events_per_second": int(os.getenv("EVENTS_PER_SECOND", "50")),
        "fraud_ratio": float(os.getenv("FRAUD_RATIO", "0.03")),
        "batch_size": int(os.getenv("BATCH_SIZE", "10")),
        "anomaly_burst_enabled": os.getenv("ANOMALY_BURST_ENABLED", "true").lower() == "true",
        "anomaly_burst_interval_sec": int(os.getenv("ANOMALY_BURST_INTERVAL_SEC", "1800")),
        "anomaly_burst_duration_sec": int(os.getenv("ANOMALY_BURST_DURATION_SEC", "60")),
        "anomaly_burst_multiplier": int(os.getenv("ANOMALY_BURST_MULTIPLIER", "5")),
        "user_pool_size": int(os.getenv("USER_POOL_SIZE", "10000")),
        "kafka_bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "metrics_port": int(os.getenv("METRICS_PORT", "8080")),
        "refund_probability": float(os.getenv("REFUND_PROBABILITY", "0.05")),
        "new_user_probability": float(os.getenv("NEW_USER_PROBABILITY", "0.02")),
    }


class DataGeneratorOrchestrator:
    """
    Orchestrates all event generators and coordinates Kafka production.
    Implements anomaly burst mode for testing pipeline alerting.
    """

    def __init__(self, settings: dict):
        self.settings = settings
        self.shutdown_event = Event()
        self.start_time = time.time()

        logger.info("initializing_generators", user_pool_size=settings["user_pool_size"])
        self.order_gen     = OrderGenerator(user_pool_size=settings["user_pool_size"])
        self.payment_gen   = PaymentGenerator(user_pool_size=settings["user_pool_size"])
        self.user_gen      = UserGenerator(user_pool_size=settings["user_pool_size"])
        self.device_gen    = DeviceGenerator(user_pool_size=settings["user_pool_size"])
        self.geo_gen       = GeoEventGenerator(user_pool_size=settings["user_pool_size"])
        self.refund_gen    = RefundGenerator(user_pool_size=settings["user_pool_size"])

        self.producer = FraudPlatformProducer(settings["kafka_bootstrap_servers"])
        logger.info("orchestrator_ready")

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        start_http_server(self.settings["metrics_port"])
        logger.info("metrics_server_started", port=self.settings["metrics_port"])

        logger.info(
            "generator_starting",
            events_per_second=self.settings["events_per_second"],
            fraud_ratio=self.settings["fraud_ratio"],
        )

        last_burst_time = time.time()
        burst_mode = False
        burst_end_time = 0.0

        while not self.shutdown_event.is_set():
            now = time.time()
            GENERATOR_UPTIME.set(now - self.start_time)

            # --- Anomaly burst logic ---
            if self.settings["anomaly_burst_enabled"]:
                if not burst_mode and (now - last_burst_time) > self.settings["anomaly_burst_interval_sec"]:
                    burst_mode = True
                    burst_end_time = now + self.settings["anomaly_burst_duration_sec"]
                    last_burst_time = now
                    logger.warning("anomaly_burst_started", duration=self.settings["anomaly_burst_duration_sec"])
                if burst_mode and now > burst_end_time:
                    burst_mode = False
                    logger.info("anomaly_burst_ended")

            effective_rate = (
                self.settings["events_per_second"] * self.settings["anomaly_burst_multiplier"]
                if burst_mode
                else self.settings["events_per_second"]
            )
            effective_fraud = (
                min(self.settings["fraud_ratio"] * 4, 0.50)
                if burst_mode
                else self.settings["fraud_ratio"]
            )

            GENERATOR_RATE.set(effective_rate)
            batch_size = self.settings["batch_size"]
            sleep_interval = batch_size / effective_rate

            try:
                self._generate_batch(batch_size, effective_fraud)
            except Exception as e:
                logger.error("batch_generation_failed", error=str(e))

            time.sleep(max(0, sleep_interval))

        logger.info("shutdown_initiated")
        self.producer.flush()
        logger.info("generator_stopped")

    def _generate_batch(self, batch_size: int, fraud_ratio: float) -> None:
        for _ in range(batch_size):
            self._generate_single_transaction(fraud_ratio)

    def _generate_single_transaction(self, fraud_ratio: float) -> None:
        """
        Generate a correlated set of events for one transaction:
        1. User event (occasionally new registration)
        2. Device fingerprint
        3. Geo-location
        4. Order
        5. Payment
        6. Refund (probabilistic)
        """
        # 1. User
        if random.random() < self.settings["new_user_probability"]:
            user_event = self.user_gen.generate_new_registration()
        else:
            user_event = self.user_gen.generate()

        session_id = str(__import__("uuid").uuid4())
        is_fraud = self.order_gen.is_fraud_scenario(
            {"risk_tier": user_event.get("risk_tier", "low")},
            fraud_ratio
        )

        # 2. Device
        device_event = self.device_gen.generate_for_user(user_event, session_id, is_fraud)

        # 3. Geo
        geo_event = self.geo_gen.generate_for_session(user_event, session_id, "order", is_fraud)

        # 4. Order
        order_event = self.order_gen.generate(fraud_ratio)
        order_event["user_id"] = user_event["user_id"]
        order_event["session_id"] = session_id
        order_event["device_id"] = device_event["device_id"]
        order_event["is_fraud"] = is_fraud

        # 5. Payment
        payment_event = self.payment_gen.generate_from_order(order_event)

        # 6. Refund (probabilistic)
        refund_event = None
        if random.random() < self.settings["refund_probability"]:
            refund_event = self.refund_gen.generate_from_order(order_event, payment_event)

        # --- Produce to Kafka ---
        self.producer.produce("users",     user_event["user_id"],    user_event)
        self.producer.produce("devices",   device_event["device_id"], device_event)
        self.producer.produce("geo_events", geo_event["event_id"],   geo_event)
        self.producer.produce("orders",    order_event["order_id"],  order_event)
        self.producer.produce("payments",  payment_event["payment_id"], payment_event)

        if refund_event:
            self.producer.produce("refunds", refund_event["refund_id"], refund_event)
            EVENTS_GENERATED.labels(event_type="refund").inc()

        for etype in ["user", "device", "geo_event", "order", "payment"]:
            EVENTS_GENERATED.labels(event_type=etype).inc()

        if is_fraud:
            FRAUD_EVENTS.inc()

    def _handle_shutdown(self, signum, frame) -> None:
        logger.info("shutdown_signal_received", signal=signum)
        self.shutdown_event.set()


def wait_for_kafka(bootstrap_servers: str, max_retries: int = 30, delay: int = 5) -> None:
    """Wait until Kafka is reachable before starting."""
    from confluent_kafka.admin import AdminClient
    logger.info("waiting_for_kafka", servers=bootstrap_servers)
    for attempt in range(max_retries):
        try:
            client = AdminClient({"bootstrap.servers": bootstrap_servers})
            client.list_topics(timeout=5)
            logger.info("kafka_connected", attempt=attempt + 1)
            return
        except Exception as e:
            logger.warning("kafka_not_ready", attempt=attempt + 1, error=str(e))
            time.sleep(delay)
    logger.error("kafka_connection_failed", max_retries=max_retries)
    sys.exit(1)


def main() -> None:
    settings = get_settings()

    logger.info("data_generator_starting", settings={
        k: v for k, v in settings.items() if "password" not in k.lower()
    })

    wait_for_kafka(settings["kafka_bootstrap_servers"])

    orchestrator = DataGeneratorOrchestrator(settings)
    orchestrator.run()


if __name__ == "__main__":
    main()

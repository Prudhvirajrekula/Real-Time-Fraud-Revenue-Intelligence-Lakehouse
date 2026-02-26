"""
Kafka producer with delivery guarantees, metrics, and error handling.
Uses JSON serialization (Schema Registry integration commented for reference).
"""

import json
import os
import time
from typing import Any

import structlog
from confluent_kafka import Producer, KafkaException
from prometheus_client import Counter, Histogram

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
MESSAGES_PRODUCED = Counter(
    "generator_messages_produced_total",
    "Total messages produced to Kafka",
    ["topic", "status"]
)
PRODUCE_LATENCY = Histogram(
    "generator_produce_latency_seconds",
    "Kafka produce latency",
    ["topic"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0]
)


def _delivery_callback(err, msg, topic: str) -> None:
    if err:
        logger.error("kafka_delivery_failed", topic=topic, error=str(err))
        MESSAGES_PRODUCED.labels(topic=topic, status="failed").inc()
    else:
        MESSAGES_PRODUCED.labels(topic=topic, status="success").inc()


class FraudPlatformProducer:
    """
    High-throughput Kafka producer with:
    - Idempotent delivery (exactly-once semantics)
    - Batching and compression
    - Prometheus instrumentation
    - Graceful shutdown
    """

    def __init__(self, bootstrap_servers: str | None = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self._producer = self._create_producer()
        self._message_count = 0
        logger.info("kafka_producer_initialized", servers=self.bootstrap_servers)

    def _create_producer(self) -> Producer:
        return Producer({
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
            "enable.idempotence": True,
            "compression.type": "lz4",
            "batch.size": 65536,
            "linger.ms": 10,
            "max.in.flight.requests.per.connection": 5,
            "socket.keepalive.enable": True,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.kbytes": 1048576,
        })

    def produce(self, topic: str, key: str, value: dict[str, Any]) -> None:
        start = time.monotonic()
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(value, default=str).encode("utf-8"),
                on_delivery=lambda err, msg: _delivery_callback(err, msg, topic),
            )
            self._message_count += 1

            # Poll to trigger callbacks and prevent queue buildup
            if self._message_count % 100 == 0:
                self._producer.poll(0)

        except BufferError:
            logger.warning("kafka_buffer_full_flushing", topic=topic)
            self._producer.flush(timeout=10)
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(value, default=str).encode("utf-8"),
            )
        except KafkaException as e:
            logger.error("kafka_produce_error", topic=topic, error=str(e))
            raise
        finally:
            PRODUCE_LATENCY.labels(topic=topic).observe(time.monotonic() - start)

    def flush(self, timeout: float = 30.0) -> None:
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning("kafka_flush_incomplete", remaining=remaining)
        else:
            logger.info("kafka_flush_complete", total_produced=self._message_count)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.flush()

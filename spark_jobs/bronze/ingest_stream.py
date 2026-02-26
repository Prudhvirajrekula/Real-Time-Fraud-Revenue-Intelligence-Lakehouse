"""
Bronze Layer - Kafka → Delta Lake Streaming Ingestion

Reads raw JSON events from all Kafka topics and writes them as-is
to the Delta Lake bronze layer. No transformations - fidelity first.

This is a long-running Structured Streaming job.
One stream per topic, all running concurrently via awaitAnyTermination().
"""

import os
import sys

sys.path.insert(0, "/opt/spark_jobs")

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, expr, from_json, from_unixtime,
    lit, to_timestamp,
)

from utils.delta_utils import get_checkpoint_path, get_delta_path
from utils.schemas import TOPIC_SCHEMAS
from utils.spark_session import get_spark_session

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
BUCKET        = os.getenv("MINIO_BUCKET", "fraud-platform")

TOPICS = ["orders", "payments", "users", "devices", "geo_events", "refunds"]

# Partition strategies per topic
PARTITION_COLUMNS = {
    "orders":     ["event_date"],
    "payments":   ["event_date"],
    "users":      ["event_date"],
    "devices":    ["event_date"],
    "geo_events": ["event_date"],
    "refunds":    ["event_date"],
}


def create_kafka_stream(spark, topic: str) -> DataFrame:
    """Create a Kafka Structured Streaming source for a single topic."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", f"spark-bronze-{topic}")
        .load()
    )


def parse_and_enrich(raw_df: DataFrame, topic: str) -> DataFrame:
    """
    Parse JSON payload, cast timestamps, add bronze metadata columns.
    Preserves raw value as _raw_payload for auditability.
    """
    schema = TOPIC_SCHEMAS[topic]

    parsed = (
        raw_df
        .select(
            col("key").cast("string").alias("_kafka_key"),
            col("topic").alias("_kafka_topic"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
            col("value").cast("string").alias("_raw_payload"),
            from_json(col("value").cast("string"), schema).alias("data"),
        )
        .select(
            col("_kafka_key"),
            col("_kafka_topic"),
            col("_kafka_partition"),
            col("_kafka_offset"),
            col("_kafka_timestamp"),
            col("_raw_payload"),
            col("data.*"),
        )
        # Bronze metadata
        .withColumn("_bronze_loaded_at", current_timestamp())
        .withColumn("_source_system", lit("kafka"))
        .withColumn("_pipeline_version", lit("1.0.0"))
        # Partition column (date extracted from event timestamp)
        .withColumn(
            "event_date",
            to_timestamp(
                from_unixtime(col("created_at") / 1000)
            ).cast("date")
        )
    )
    return parsed


def write_to_bronze(df: DataFrame, topic: str) -> object:
    """Write parsed stream to Delta Lake bronze layer."""
    delta_path      = get_delta_path("bronze", topic)
    checkpoint_path = get_checkpoint_path("bronze", topic)
    partition_cols  = PARTITION_COLUMNS.get(topic, ["event_date"])

    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .partitionBy(*partition_cols)
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime="30 seconds")
        .start(delta_path)
    )


def main():
    spark = get_spark_session(
        app_name="FraudPlatform-Bronze-Ingestion",
        enable_delta=True,
        enable_kafka=True,
        driver_memory="2g",
        executor_memory="2g",
    )

    print(f"Starting bronze ingestion streams for topics: {TOPICS}")
    queries = []

    for topic in TOPICS:
        print(f"  Starting stream: {topic}")
        raw_df  = create_kafka_stream(spark, topic)
        clean_df = parse_and_enrich(raw_df, topic)
        query   = write_to_bronze(clean_df, topic)
        queries.append(query)
        print(f"  Stream started: {topic} → s3a://{BUCKET}/bronze/{topic}")

    print(f"All {len(queries)} bronze streams running. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

"""
Delta Lake utility functions for the fraud platform.
Handles upserts (MERGE), schema evolution, optimization, and time travel.
"""

import os
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


BUCKET = os.getenv("MINIO_BUCKET", "fraud-platform")
BASE_PATH = f"s3a://{BUCKET}"


def get_delta_path(layer: str, table: str) -> str:
    """Return the S3A path for a Delta table."""
    return f"{BASE_PATH}/{layer}/{table}"


def get_checkpoint_path(layer: str, table: str) -> str:
    """Return the S3A checkpoint path for Structured Streaming."""
    return f"{BASE_PATH}/checkpoints/{layer}/{table}"


def write_delta_batch(
    df: DataFrame,
    layer: str,
    table: str,
    mode: str = "append",
    partition_cols: Optional[list[str]] = None,
) -> None:
    """Write a batch DataFrame to Delta Lake."""
    path = get_delta_path(layer, table)
    writer = df.write.format("delta").mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)


def upsert_delta(
    spark: SparkSession,
    updates_df: DataFrame,
    layer: str,
    table: str,
    merge_keys: list[str],
    partition_cols: Optional[list[str]] = None,
) -> None:
    """
    Perform a Delta MERGE (upsert) operation.
    Creates the table if it doesn't exist.
    """
    path = get_delta_path(layer, table)

    if not DeltaTable.isDeltaTable(spark, path):
        write_delta_batch(updates_df, layer, table, mode="overwrite", partition_cols=partition_cols)
        return

    delta_table = DeltaTable.forPath(spark, path)
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    (
        delta_table.alias("target")
        .merge(updates_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def read_delta(spark: SparkSession, layer: str, table: str) -> DataFrame:
    """Read a Delta table."""
    path = get_delta_path(layer, table)
    return spark.read.format("delta").load(path)


def read_delta_as_of(
    spark: SparkSession,
    layer: str,
    table: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
) -> DataFrame:
    """Read a Delta table at a specific version or timestamp (time travel)."""
    path = get_delta_path(layer, table)
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    return reader.load(path)


def optimize_delta_table(spark: SparkSession, layer: str, table: str) -> None:
    """Run OPTIMIZE + ZORDER for query performance."""
    path = get_delta_path(layer, table)
    spark.sql(f"OPTIMIZE delta.`{path}`")


def vacuum_delta_table(
    spark: SparkSession,
    layer: str,
    table: str,
    retain_hours: int = 168,
) -> None:
    """Remove old Delta Lake files (default: keep 7 days)."""
    path = get_delta_path(layer, table)
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")


def get_table_history(spark: SparkSession, layer: str, table: str) -> DataFrame:
    """Return Delta table history for auditing."""
    path = get_delta_path(layer, table)
    delta_table = DeltaTable.forPath(spark, path)
    return delta_table.history()

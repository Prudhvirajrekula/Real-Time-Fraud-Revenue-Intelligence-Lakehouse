"""
Silver Layer - Fraud Feature Engineering

Builds the ML feature table by computing:
- Transaction velocity features (1h, 24h, 7d windows)
- User behavioral features (historical averages, anomaly flags)
- Device risk features
- Geo risk features

Output: silver/fraud_features (used by ML training pipeline)
"""

import os
import sys

sys.path.insert(0, "/opt/spark_jobs")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg, col, count, countDistinct, current_timestamp, datediff,
    exp, lag, lit, log as spark_log, max as spark_max, min as spark_min,
    ntile, percentile_approx, round as spark_round, stddev, sum as spark_sum,
    to_date, when, window as time_window, coalesce, from_unixtime, to_timestamp,
)
from pyspark.sql.window import Window

from utils.delta_utils import get_delta_path, write_delta_batch
from utils.spark_session import get_spark_session

HIGH_RISK_COUNTRIES = {"NG", "RU", "CN", "MX", "ZA", "PH", "BR"}

COUNTRY_RISK_MAP = {
    "US": 0.02, "GB": 0.025, "DE": 0.015, "FR": 0.02, "CA": 0.02,
    "AU": 0.025, "JP": 0.01, "BR": 0.055, "IN": 0.04, "NG": 0.18,
    "RU": 0.12, "CN": 0.06, "MX": 0.07, "ZA": 0.08, "PH": 0.09,
}


def compute_transaction_features(orders_enriched: DataFrame) -> DataFrame:
    """
    Compute transaction-level features:
    - Log-transformed amount
    - Hour of day, day of week, is_weekend
    - Amount relative to user's historical average and P95
    """
    user_window = Window.partitionBy("user_id")
    user_ordered = Window.partitionBy("user_id").orderBy("created_ts")

    return (
        orders_enriched
        .withColumn("amount_log", spark_log(col("total_amount") + lit(1.0)))
        .withColumn("hour_of_day", col("created_ts").cast("timestamp").cast("long") % 86400 / 3600)
        .withColumn("day_of_week", (col("created_ts").cast("date").cast("long") / 86400 + 4) % 7)
        .withColumn("is_weekend", ((col("day_of_week") >= 5).cast("int")))
        .withColumn("user_avg_amount", avg("total_amount").over(user_window))
        .withColumn("user_p95_amount", percentile_approx("total_amount", 0.95).over(user_window))
        .withColumn(
            "amount_vs_user_avg",
            spark_round(col("total_amount") / (col("user_avg_amount") + lit(0.01)), 4)
        )
        .withColumn(
            "amount_vs_user_p95",
            spark_round(col("total_amount") / (col("user_p95_amount") + lit(0.01)), 4)
        )
    )


def compute_velocity_features(orders_enriched: DataFrame) -> DataFrame:
    """
    Transaction velocity: count per user in 1h, 24h, 7d rolling windows.
    Uses a self-join approximation since PySpark streaming windows
    require event_time which we have via created_ts.
    """
    # We use a time-based window aggregation + join approach
    orders_ts = orders_enriched.withColumn(
        "ts_epoch", col("created_at").cast("long")
    )

    base = orders_ts.select(
        "order_id", "user_id", "created_at", "ts_epoch", "total_amount"
    ).alias("base")

    history = orders_ts.select(
        col("user_id").alias("h_user_id"),
        col("ts_epoch").alias("h_ts_epoch"),
        col("total_amount").alias("h_amount"),
    ).alias("history")

    # velocity_1h: orders by same user within 1 hour (3600000 ms)
    v1h = (
        base.join(
            history,
            (base["user_id"] == history["h_user_id"]) &
            (history["h_ts_epoch"] >= base["ts_epoch"] - 3_600_000) &
            (history["h_ts_epoch"] < base["ts_epoch"]),
            "left"
        )
        .groupBy("order_id")
        .agg(count("h_user_id").alias("velocity_1h"))
    )

    # velocity_24h
    v24h = (
        base.join(
            history,
            (base["user_id"] == history["h_user_id"]) &
            (history["h_ts_epoch"] >= base["ts_epoch"] - 86_400_000) &
            (history["h_ts_epoch"] < base["ts_epoch"]),
            "left"
        )
        .groupBy("order_id")
        .agg(count("h_user_id").alias("velocity_24h"))
    )

    # velocity_7d
    v7d = (
        base.join(
            history,
            (base["user_id"] == history["h_user_id"]) &
            (history["h_ts_epoch"] >= base["ts_epoch"] - 604_800_000) &
            (history["h_ts_epoch"] < base["ts_epoch"]),
            "left"
        )
        .groupBy("order_id")
        .agg(count("h_user_id").alias("velocity_7d"))
    )

    return (
        orders_enriched
        .join(v1h,  "order_id", "left")
        .join(v24h, "order_id", "left")
        .join(v7d,  "order_id", "left")
        .fillna({"velocity_1h": 0, "velocity_24h": 0, "velocity_7d": 0})
    )


def compute_user_features(
    orders_enriched: DataFrame,
    user_profiles: DataFrame,
    refunds_df: DataFrame,
) -> DataFrame:
    """Merge user profile stats into the feature table."""
    user_stats = (
        user_profiles.select(
            "user_id",
            "account_age_days",
            "risk_tier",
            "risk_tier_encoded",
            "chargeback_count",
            "total_orders_lifetime",
            "total_spend_lifetime",
            "is_high_risk_country",
            "country_risk_score",
        )
    )

    # Recent refund rate per user (30d)
    refund_stats = (
        refunds_df
        .groupBy("user_id")
        .agg(
            count("refund_id").alias("refund_count_30d"),
            spark_sum(when(col("is_fraud_related"), 1).otherwise(0)).alias("fraud_refund_count"),
        )
    )

    return (
        orders_enriched
        .join(user_stats, "user_id", "left")
        .join(refund_stats, "user_id", "left")
        .fillna({
            "refund_count_30d": 0,
            "fraud_refund_count": 0,
            "account_age_days": 0,
            "risk_tier_encoded": 0,
        })
        .withColumn(
            "chargeback_rate",
            spark_round(
                col("chargeback_count") / (col("total_orders_lifetime") + lit(1)),
                4
            )
        )
    )


def compute_device_features(
    feature_df: DataFrame,
    devices_df: DataFrame,
) -> DataFrame:
    """Add device risk signals."""
    device_stats = (
        devices_df
        .groupBy("user_id")
        .agg(
            countDistinct("device_id").alias("device_count_30d"),
            spark_sum(when(col("vpn_detected"), 1).otherwise(0)).alias("vpn_count"),
            spark_sum(when(col("tor_detected"), 1).otherwise(0)).alias("tor_count"),
        )
    )

    # Get latest device per session
    device_latest = devices_df.select(
        "session_id",
        col("vpn_detected").alias("dev_vpn_detected"),
        col("proxy_detected").alias("dev_proxy_detected"),
        col("tor_detected").alias("dev_tor_detected"),
        col("bot_detected").alias("dev_bot_detected"),
    ).dropDuplicates(["session_id"])

    return (
        feature_df
        .join(device_stats, "user_id", "left")
        .join(device_latest, "session_id", "left")
        .fillna({
            "device_count_30d": 1,
            "vpn_count": 0,
            "tor_count": 0,
            "dev_vpn_detected": False,
            "dev_proxy_detected": False,
            "dev_tor_detected": False,
            "dev_bot_detected": False,
        })
    )


def build_final_feature_vector(df: DataFrame) -> DataFrame:
    """
    Select and finalize the feature vector for ML training.
    All features must be numeric for XGBoost.
    """
    return (
        df.select(
            # Keys
            col("order_id"),
            col("user_id"),
            # Label
            coalesce(col("is_fraud"), lit(False)).alias("label"),
            # Transaction features
            col("total_amount"),
            col("amount_log"),
            spark_round(col("hour_of_day"), 2).alias("hour_of_day"),
            col("day_of_week").cast("int"),
            col("is_weekend"),
            coalesce(col("amount_vs_user_avg"), lit(1.0)).alias("amount_vs_user_avg"),
            coalesce(col("amount_vs_user_p95"), lit(1.0)).alias("amount_vs_user_p95"),
            coalesce(col("velocity_1h"),  lit(0)).alias("velocity_1h"),
            coalesce(col("velocity_24h"), lit(0)).alias("velocity_24h"),
            coalesce(col("velocity_7d"),  lit(0)).alias("velocity_7d"),
            # User features
            coalesce(col("account_age_days"), lit(0)).alias("account_age_days"),
            coalesce(col("risk_tier_encoded"), lit(0)).alias("risk_tier_encoded"),
            coalesce(col("chargeback_count"), lit(0)).alias("chargeback_count"),
            coalesce(col("chargeback_rate"),  lit(0.0)).alias("chargeback_rate"),
            coalesce(col("refund_count_30d"), lit(0)).alias("refund_count_30d"),
            coalesce(col("fraud_refund_count"), lit(0)).alias("fraud_refund_count"),
            # Device features
            coalesce(col("dev_vpn_detected"),   lit(False)).cast("int").alias("vpn_detected"),
            coalesce(col("dev_proxy_detected"),  lit(False)).cast("int").alias("proxy_detected"),
            coalesce(col("dev_tor_detected"),    lit(False)).cast("int").alias("tor_detected"),
            coalesce(col("dev_bot_detected"),    lit(False)).cast("int").alias("bot_detected"),
            coalesce(col("device_count_30d"),    lit(1)).alias("device_count_30d"),
            # Geo features
            coalesce(col("geo_mismatch_flag"),           lit(False)).cast("int").alias("geo_mismatch"),
            coalesce(col("is_high_risk_shipping"),        lit(False)).cast("int").alias("is_high_risk_country"),
            coalesce(col("shipping_country_risk"),        lit(0.05)).alias("country_risk_score"),
            # Payment features
            coalesce(col("risk_score"),         lit(0.1)).alias("payment_risk_score"),
            coalesce(col("is_3ds_authenticated"), lit(True)).cast("int").alias("is_3ds_authenticated"),
            # Partition
            col("event_date"),
            current_timestamp().alias("_feature_computed_at"),
        )
    )


def main():
    spark = get_spark_session(
        app_name="FraudPlatform-Silver-FraudFeatures",
        enable_delta=True,
    )

    print("Reading silver/gold inputs...")
    orders_enriched = spark.read.format("delta").load(get_delta_path("silver", "orders_enriched"))
    user_profiles   = spark.read.format("delta").load(get_delta_path("silver", "user_profiles"))
    devices_df      = spark.read.format("delta").load(get_delta_path("bronze", "devices"))
    refunds_df      = spark.read.format("delta").load(get_delta_path("bronze", "refunds"))

    print("Computing transaction features...")
    df = compute_transaction_features(orders_enriched)

    print("Computing velocity features...")
    df = compute_velocity_features(df)

    print("Joining user features...")
    df = compute_user_features(df, user_profiles, refunds_df)

    print("Joining device features...")
    df = compute_device_features(df, devices_df)

    print("Building final feature vector...")
    feature_df = build_final_feature_vector(df)

    print("Writing to silver/fraud_features...")
    write_delta_batch(
        feature_df, "silver", "fraud_features",
        mode="overwrite",
        partition_cols=["event_date"],
    )

    count = feature_df.count()
    fraud_count = feature_df.filter(col("label") == True).count()
    print(f"Feature table written: {count} rows, {fraud_count} fraud ({100*fraud_count/count:.2f}%)")
    spark.stop()


if __name__ == "__main__":
    main()

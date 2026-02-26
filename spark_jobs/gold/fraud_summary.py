"""
Gold Layer - Fraud Intelligence Summary

Builds fraud analytics tables:
1. fraud_summary_daily - Daily fraud KPIs
2. user_fraud_scores   - Per-user rolling fraud risk scores
3. fraud_patterns      - Fraud breakdown by channel, method, geo
"""

import os
import sys

sys.path.insert(0, "/opt/spark_jobs")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, lit, round as spark_round,
    sum as spark_sum, when, window as time_window, to_date,
    percentile_approx, stddev, coalesce,
)
from pyspark.sql.window import Window

from utils.delta_utils import get_delta_path, write_delta_batch
from utils.spark_session import get_spark_session

POSTGRES_JDBC = os.environ["POSTGRES_CONN_STRING"].replace("postgresql://", "jdbc:postgresql://")
POSTGRES_PROPS = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}


def write_to_postgres(df: DataFrame, table: str) -> None:
    df.write.jdbc(url=POSTGRES_JDBC, table=f"gold.{table}", mode="overwrite", properties=POSTGRES_PROPS)


def build_fraud_summary_daily(
    orders_enriched: DataFrame,
    refunds_df: DataFrame,
) -> DataFrame:
    """
    Daily fraud KPIs:
    - Fraud rate by country, payment method, amount tier
    - Chargeback rate
    - Refund rate
    """
    fraud_base = (
        orders_enriched
        .groupBy("event_date", "shipping_country", "payment_method", "amount_tier")
        .agg(
            count("order_id").alias("total_orders"),
            count(when(col("is_fraud") == True, True)).alias("fraud_orders"),
            spark_sum(when(col("is_fraud") == True, col("total_amount")).otherwise(0)).alias("fraud_gmv"),
            spark_sum("total_amount").alias("total_gmv"),
            avg(when(col("is_fraud") == True, col("total_amount"))).alias("avg_fraud_amount"),
            count(when(col("geo_mismatch_flag") == True, True)).alias("geo_mismatch_count"),
            count(when(col("dev_vpn_detected") == True, True)).alias("vpn_orders"),
        )
        .withColumn("fraud_rate",
            spark_round(col("fraud_orders") / (col("total_orders") + lit(0.001)), 4)
        )
        .withColumn("fraud_gmv_rate",
            spark_round(col("fraud_gmv") / (col("total_gmv") + lit(0.01)), 4)
        )
    )

    # Refund stats per day/country
    refund_stats = (
        refunds_df
        .withColumn("refund_date", to_date(
            (col("created_at") / 1000).cast("timestamp")
        ))
        .groupBy("refund_date")
        .agg(
            count("refund_id").alias("total_refunds"),
            spark_sum("amount").alias("total_refund_amount"),
            count(when(col("is_fraud_related") == True, True)).alias("fraud_refunds"),
        )
        .withColumnRenamed("refund_date", "event_date")
    )

    return (
        fraud_base
        .join(refund_stats, "event_date", "left")
        .fillna({"total_refunds": 0, "total_refund_amount": 0.0, "fraud_refunds": 0})
        .withColumn("_computed_at", current_timestamp())
    )


def build_user_fraud_scores(
    orders_enriched: DataFrame,
    feature_df: DataFrame,
) -> DataFrame:
    """
    Per-user aggregated fraud risk profile (last 30 days).
    Used for real-time enrichment of incoming transactions.
    """
    user_window = Window.partitionBy("user_id")

    return (
        feature_df
        .groupBy("user_id")
        .agg(
            count("order_id").alias("orders_30d"),
            spark_sum(col("label").cast("int")).alias("fraud_count_30d"),
            avg("payment_risk_score").alias("avg_risk_score"),
            avg("velocity_24h").alias("avg_velocity_24h"),
            spark_sum(col("vpn_detected")).alias("vpn_sessions_30d"),
            spark_sum(col("geo_mismatch")).alias("geo_mismatches_30d"),
            avg("amount_vs_user_avg").alias("avg_amount_deviation"),
        )
        .withColumn(
            "user_fraud_rate",
            spark_round(col("fraud_count_30d") / (col("orders_30d") + lit(0.001)), 4)
        )
        .withColumn(
            "composite_risk_score",
            spark_round(
                col("avg_risk_score") * lit(0.40)
                + col("user_fraud_rate") * lit(0.35)
                + (col("vpn_sessions_30d") / (col("orders_30d") + lit(1))) * lit(0.15)
                + (col("geo_mismatches_30d") / (col("orders_30d") + lit(1))) * lit(0.10),
                4
            )
        )
        .withColumn(
            "risk_label",
            when(col("composite_risk_score") >= 0.50, lit("high"))
            .when(col("composite_risk_score") >= 0.20, lit("medium"))
            .otherwise(lit("low"))
        )
        .withColumn("_computed_at", current_timestamp())
    )


def main():
    spark = get_spark_session(
        app_name="FraudPlatform-Gold-FraudSummary",
        enable_delta=True,
    )

    print("Reading inputs...")
    orders_enriched = spark.read.format("delta").load(get_delta_path("silver", "orders_enriched"))
    refunds_df      = spark.read.format("delta").load(get_delta_path("bronze", "refunds"))
    feature_df      = spark.read.format("delta").load(get_delta_path("silver", "fraud_features"))

    print("Building fraud_summary_daily...")
    fraud_daily = build_fraud_summary_daily(orders_enriched, refunds_df)
    write_delta_batch(fraud_daily, "gold", "fraud_summary", mode="overwrite", partition_cols=["event_date"])
    write_to_postgres(fraud_daily, "fraud_summary")
    print(f"  fraud_summary: {fraud_daily.count()} rows")

    print("Building user_fraud_scores...")
    user_scores = build_user_fraud_scores(orders_enriched, feature_df)
    write_delta_batch(user_scores, "gold", "user_fraud_scores", mode="overwrite")
    write_to_postgres(user_scores, "user_fraud_scores")
    print(f"  user_fraud_scores: {user_scores.count()} rows")

    print("Gold fraud summary complete.")
    spark.stop()


if __name__ == "__main__":
    main()

"""
Silver Layer - Transaction Enrichment & Cleansing

Reads bronze Delta tables, applies:
1. Data quality filters (null removal, type casting)
2. Business rule enrichment (geo risk, billing/shipping mismatch)
3. Denormalized order+payment join
4. User profile snapshot
5. Writes to silver Delta layer (partitioned by event_date)

Run: batch job, triggered by Airflow on schedule.
"""

import os
import sys

sys.path.insert(0, "/opt/spark_jobs")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs, col, datediff, expr, lit, round as spark_round,
    to_date, to_timestamp, udf, when, from_unixtime, coalesce, upper,
    concat_ws, sha2, current_timestamp,
)
from pyspark.sql.types import DoubleType, StringType

from utils.delta_utils import get_delta_path, upsert_delta, write_delta_batch
from utils.spark_session import get_spark_session

# ---------------------------------------------------------------------------
# Country risk scores (simulated - in production use a real risk DB)
# ---------------------------------------------------------------------------
COUNTRY_RISK_SCORES = {
    "US": 0.02, "GB": 0.025, "DE": 0.015, "FR": 0.02, "CA": 0.02,
    "AU": 0.025, "JP": 0.01, "BR": 0.055, "IN": 0.04, "NG": 0.18,
    "RU": 0.12, "CN": 0.06, "MX": 0.07, "ZA": 0.08, "PH": 0.09,
}
HIGH_RISK_THRESHOLD = 0.05


def get_country_risk_udf():
    scores = COUNTRY_RISK_SCORES
    threshold = HIGH_RISK_THRESHOLD

    def _get_risk(country: str) -> float:
        return scores.get(country, 0.05)

    return udf(_get_risk, DoubleType())


def clean_orders(orders_df: DataFrame) -> DataFrame:
    """Clean and validate bronze orders."""
    return (
        orders_df
        .filter(col("order_id").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("total_amount") > 0)
        .filter(col("total_amount") < 100_000)  # Business rule: cap at 100k
        .dropDuplicates(["order_id"])
        .withColumn("total_amount", spark_round(col("total_amount"), 2))
        .withColumn("currency", upper(col("currency")))
        .withColumn(
            "order_status_normalized",
            when(col("order_status").isin("pending", "confirmed", "shipped", "delivered", "cancelled"),
                 col("order_status"))
            .otherwise(lit("unknown"))
        )
        .withColumn(
            "created_ts",
            to_timestamp(from_unixtime(col("created_at") / 1000))
        )
    )


def clean_payments(payments_df: DataFrame) -> DataFrame:
    """Clean and validate bronze payments."""
    return (
        payments_df
        .filter(col("payment_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("amount") > 0)
        .dropDuplicates(["payment_id"])
        .withColumn("amount", spark_round(col("amount"), 2))
        .withColumn("risk_score", col("risk_score").cast(DoubleType()))
        .withColumn(
            "created_ts",
            to_timestamp(from_unixtime(col("created_at") / 1000))
        )
    )


def enrich_orders(orders_df: DataFrame) -> DataFrame:
    """Add business-derived enrichment columns."""
    country_risk = get_country_risk_udf()
    return (
        orders_df
        # Geo risk
        .withColumn("shipping_country_risk", country_risk(col("shipping_country")))
        .withColumn("billing_country_risk",  country_risk(col("billing_country")))
        .withColumn(
            "is_high_risk_shipping",
            col("shipping_country_risk") > lit(HIGH_RISK_THRESHOLD)
        )
        # Billing/shipping mismatch is a strong fraud signal
        .withColumn(
            "geo_mismatch_flag",
            col("shipping_country") != col("billing_country")
        )
        # Amount tiers
        .withColumn(
            "amount_tier",
            when(col("total_amount") < 50,   lit("micro"))
            .when(col("total_amount") < 200,  lit("low"))
            .when(col("total_amount") < 1000, lit("medium"))
            .when(col("total_amount") < 5000, lit("high"))
            .otherwise(lit("premium"))
        )
        # IP masking for PII compliance
        .withColumn(
            "ip_address_masked",
            concat_ws(".", expr("split(ip_address, '\\\\.')[0]"),
                           expr("split(ip_address, '\\\\.')[1]"), lit("*"), lit("*"))
        )
    )


def enrich_payments(payments_df: DataFrame) -> DataFrame:
    """Add payment-specific enrichments."""
    return (
        payments_df
        .withColumn(
            "is_high_risk_score",
            col("risk_score") > lit(0.65)
        )
        .withColumn(
            "payment_outcome",
            when(col("payment_status") == "completed", lit("success"))
            .when(col("payment_status").isin("failed", "declined"), lit("failure"))
            .otherwise(lit("pending"))
        )
        .withColumn(
            "effective_amount",
            col("amount") - coalesce(col("processor_fee"), lit(0.0))
        )
    )


def build_orders_enriched(
    spark: SparkSession,
    orders_df: DataFrame,
    payments_df: DataFrame,
) -> DataFrame:
    """
    Join orders with their payments to create a denormalized
    orders_enriched silver table.
    """
    clean_ord = enrich_orders(clean_orders(orders_df))
    clean_pay = enrich_payments(clean_payments(payments_df))

    # Select payment columns for join (avoid column name collisions)
    pay_cols = clean_pay.select(
        col("payment_id"),
        col("order_id").alias("pay_order_id"),
        col("amount").alias("payment_amount"),
        col("payment_status"),
        col("payment_method").alias("pay_method"),
        col("card_brand"),
        col("card_country"),
        col("gateway"),
        col("risk_score"),
        col("is_3ds_authenticated"),
        col("processor_fee"),
        col("is_high_risk_score"),
        col("payment_outcome"),
        col("effective_amount"),
        col("is_fraud").alias("payment_is_fraud"),
    )

    enriched = (
        clean_ord.alias("o")
        .join(pay_cols.alias("p"), clean_ord["order_id"] == pay_cols["pay_order_id"], "left")
        .drop("pay_order_id")
        .withColumn("_silver_transformed_at", current_timestamp())
        .withColumn("event_date", to_date(col("created_ts")))
    )
    return enriched


def build_user_profiles(
    spark: SparkSession,
    users_df: DataFrame,
) -> DataFrame:
    """
    Build deduplicated user profiles (latest record per user).
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window = Window.partitionBy("user_id").orderBy(col("last_login_at").desc())

    country_risk = get_country_risk_udf()

    return (
        users_df
        .filter(col("user_id").isNotNull())
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .withColumn("country_risk_score", country_risk(col("country")))
        .withColumn("is_high_risk_country", col("country_risk_score") > HIGH_RISK_THRESHOLD)
        .withColumn(
            "risk_tier_encoded",
            when(col("risk_tier") == "high",   lit(2))
            .when(col("risk_tier") == "medium", lit(1))
            .otherwise(lit(0))
        )
        .withColumn(
            "last_login_ts",
            to_timestamp(from_unixtime(col("last_login_at") / 1000))
        )
        .withColumn("_silver_transformed_at", current_timestamp())
        .withColumn("event_date", to_date(col("last_login_ts")))
    )


def main():
    spark = get_spark_session(
        app_name="FraudPlatform-Silver-Transformations",
        enable_delta=True,
    )

    print("Reading bronze layer...")
    orders_df   = spark.read.format("delta").load(get_delta_path("bronze", "orders"))
    payments_df = spark.read.format("delta").load(get_delta_path("bronze", "payments"))
    users_df    = spark.read.format("delta").load(get_delta_path("bronze", "users"))

    print("Building orders_enriched...")
    orders_enriched = build_orders_enriched(spark, orders_df, payments_df)
    upsert_delta(
        spark, orders_enriched, "silver", "orders_enriched",
        merge_keys=["order_id"],
        partition_cols=["event_date"],
    )
    print(f"  Written {orders_enriched.count()} rows to silver/orders_enriched")

    print("Building user_profiles...")
    user_profiles = build_user_profiles(spark, users_df)
    upsert_delta(
        spark, user_profiles, "silver", "user_profiles",
        merge_keys=["user_id"],
        partition_cols=["event_date"],
    )
    print(f"  Written {user_profiles.count()} rows to silver/user_profiles")

    print("Silver transformation complete.")
    spark.stop()


if __name__ == "__main__":
    main()

"""
Gold Layer - Revenue & Business Intelligence Aggregations

Computes:
1. revenue_daily   - Daily revenue KPIs by country + payment method
2. revenue_hourly  - Hourly revenue for real-time dashboards
3. product_metrics - Revenue and volume by product category

Also writes to PostgreSQL analytics warehouse for dbt access.
"""

import os
import sys

sys.path.insert(0, "/opt/spark_jobs")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg, col, count, countDistinct, current_timestamp, lit,
    round as spark_round, sum as spark_sum, to_date, to_timestamp,
    when, date_trunc, hour, coalesce, percentile_approx,
)

from utils.delta_utils import get_delta_path, write_delta_batch
from utils.spark_session import get_spark_session

POSTGRES_CONN = os.environ["POSTGRES_CONN_STRING"]
POSTGRES_JDBC = POSTGRES_CONN.replace("postgresql://", "jdbc:postgresql://")
POSTGRES_PROPS = {
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}


def write_to_postgres(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """Write DataFrame to PostgreSQL analytics schema."""
    df.write.jdbc(
        url=POSTGRES_JDBC,
        table=f"gold.{table}",
        mode=mode,
        properties=POSTGRES_PROPS,
    )


def build_revenue_daily(orders_enriched: DataFrame) -> DataFrame:
    """
    Daily revenue metrics:
    - GMV (Gross Merchandise Value)
    - Successful payment revenue
    - Fraud amount
    - Order counts, fraud rates
    """
    return (
        orders_enriched
        .filter(col("event_date").isNotNull())
        .groupBy(
            "event_date",
            "shipping_country",
            "currency",
            "payment_method",
            "amount_tier",
        )
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum("total_amount").alias("gmv"),
            spark_sum(
                when(col("payment_outcome") == "success", col("total_amount")).otherwise(0)
            ).alias("net_revenue"),
            spark_sum(
                when(col("is_fraud") == True, col("total_amount")).otherwise(0)
            ).alias("fraud_amount"),
            count(when(col("is_fraud") == True, True)).alias("fraud_orders"),
            count(when(col("payment_outcome") == "failure", True)).alias("failed_payments"),
            avg("total_amount").alias("avg_order_value"),
            percentile_approx("total_amount", 0.50).alias("median_order_value"),
            percentile_approx("total_amount", 0.95).alias("p95_order_value"),
            countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("fraud_rate",
            spark_round(col("fraud_orders") / (col("total_orders") + lit(0.001)), 4)
        )
        .withColumn("payment_failure_rate",
            spark_round(col("failed_payments") / (col("total_orders") + lit(0.001)), 4)
        )
        .withColumn("gmv", spark_round(col("gmv"), 2))
        .withColumn("net_revenue", spark_round(col("net_revenue"), 2))
        .withColumn("fraud_amount", spark_round(col("fraud_amount"), 2))
        .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
        .withColumn("_computed_at", current_timestamp())
    )


def build_revenue_hourly(orders_enriched: DataFrame) -> DataFrame:
    """Hourly revenue aggregation for real-time dashboards."""
    return (
        orders_enriched
        .withColumn("event_hour", date_trunc("hour", col("created_ts")))
        .filter(col("event_hour").isNotNull())
        .groupBy("event_hour", "shipping_country")
        .agg(
            count("order_id").alias("order_count"),
            spark_sum("total_amount").alias("hourly_gmv"),
            count(when(col("is_fraud") == True, True)).alias("fraud_count"),
            avg("total_amount").alias("avg_amount"),
        )
        .withColumn("fraud_rate",
            spark_round(col("fraud_count") / (col("order_count") + lit(0.001)), 4)
        )
        .withColumn("hourly_gmv", spark_round(col("hourly_gmv"), 2))
        .withColumn("_computed_at", current_timestamp())
    )


def build_product_metrics(orders_enriched: DataFrame) -> DataFrame:
    """Revenue and volume metrics by product category."""
    from pyspark.sql.functions import explode, size

    return (
        orders_enriched
        .withColumn("item", explode(col("items")))
        .groupBy(
            "event_date",
            col("item.product_category").alias("category"),
        )
        .agg(
            count("order_id").alias("order_count"),
            spark_sum(
                col("item.unit_price") * col("item.quantity")
            ).alias("category_revenue"),
            spark_sum("item.quantity").alias("units_sold"),
            avg("item.unit_price").alias("avg_unit_price"),
            count(when(col("is_fraud") == True, True)).alias("fraud_orders"),
        )
        .withColumn("category_revenue", spark_round(col("category_revenue"), 2))
        .withColumn("avg_unit_price", spark_round(col("avg_unit_price"), 2))
        .withColumn("_computed_at", current_timestamp())
    )


def main():
    spark = get_spark_session(
        app_name="FraudPlatform-Gold-RevenueAggregations",
        enable_delta=True,
    )

    print("Reading silver/orders_enriched...")
    orders_enriched = spark.read.format("delta").load(get_delta_path("silver", "orders_enriched"))

    print("Building revenue_daily...")
    revenue_daily = build_revenue_daily(orders_enriched)
    write_delta_batch(revenue_daily, "gold", "revenue_daily", mode="overwrite", partition_cols=["event_date"])
    write_to_postgres(revenue_daily, "revenue_daily")
    print(f"  revenue_daily: {revenue_daily.count()} rows")

    print("Building revenue_hourly...")
    revenue_hourly = build_revenue_hourly(orders_enriched)
    write_delta_batch(revenue_hourly, "gold", "revenue_hourly", mode="overwrite")
    write_to_postgres(revenue_hourly, "revenue_hourly")
    print(f"  revenue_hourly: {revenue_hourly.count()} rows")

    print("Building product_metrics...")
    product_metrics = build_product_metrics(orders_enriched)
    write_delta_batch(product_metrics, "gold", "product_metrics", mode="overwrite", partition_cols=["event_date"])
    write_to_postgres(product_metrics, "product_metrics")
    print(f"  product_metrics: {product_metrics.count()} rows")

    print("Gold revenue aggregations complete.")
    spark.stop()


if __name__ == "__main__":
    main()

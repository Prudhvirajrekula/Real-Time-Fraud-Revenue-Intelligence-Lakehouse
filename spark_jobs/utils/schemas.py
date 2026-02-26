"""
PySpark schemas for all event types.
Single source of truth for schema definitions across bronze/silver/gold layers.
"""

from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, FloatType, IntegerType,
    LongType, StringType, StructField, StructType, TimestampType,
)

# ---------------------------------------------------------------------------
# Nested types
# ---------------------------------------------------------------------------
ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id",       StringType(), False),
    StructField("product_category", StringType(), True),
    StructField("quantity",         IntegerType(), False),
    StructField("unit_price",       FloatType(),  False),
])

# ---------------------------------------------------------------------------
# Raw Kafka message schemas (from JSON)
# ---------------------------------------------------------------------------
ORDER_SCHEMA = StructType([
    StructField("order_id",         StringType(),  False),
    StructField("user_id",          StringType(),  False),
    StructField("session_id",       StringType(),  True),
    StructField("device_id",        StringType(),  True),
    StructField("items",            ArrayType(ORDER_ITEM_SCHEMA), True),
    StructField("total_amount",     DoubleType(),  False),
    StructField("currency",         StringType(),  True),
    StructField("order_status",     StringType(),  True),
    StructField("payment_method",   StringType(),  True),
    StructField("shipping_country", StringType(),  True),
    StructField("shipping_city",    StringType(),  True),
    StructField("billing_country",  StringType(),  True),
    StructField("ip_address",       StringType(),  True),
    StructField("user_agent",       StringType(),  True),
    StructField("is_fraud",         BooleanType(), True),
    StructField("created_at",       LongType(),    True),
    StructField("ingest_timestamp", LongType(),    True),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id",              StringType(),  False),
    StructField("order_id",                StringType(),  False),
    StructField("user_id",                 StringType(),  False),
    StructField("amount",                  DoubleType(),  False),
    StructField("currency",                StringType(),  True),
    StructField("payment_status",          StringType(),  True),
    StructField("payment_method",          StringType(),  True),
    StructField("card_last_four",          StringType(),  True),
    StructField("card_brand",              StringType(),  True),
    StructField("card_country",            StringType(),  True),
    StructField("gateway",                 StringType(),  True),
    StructField("gateway_transaction_id",  StringType(),  True),
    StructField("gateway_response_code",   StringType(),  True),
    StructField("risk_score",              FloatType(),   True),
    StructField("is_3ds_authenticated",    BooleanType(), True),
    StructField("processor_fee",           DoubleType(),  True),
    StructField("is_fraud",                BooleanType(), True),
    StructField("created_at",              LongType(),    True),
    StructField("ingest_timestamp",        LongType(),    True),
])

USER_SCHEMA = StructType([
    StructField("user_id",               StringType(),  False),
    StructField("email",                 StringType(),  True),
    StructField("username",              StringType(),  True),
    StructField("first_name",            StringType(),  True),
    StructField("last_name",             StringType(),  True),
    StructField("country",               StringType(),  True),
    StructField("account_age_days",      IntegerType(), True),
    StructField("is_verified",           BooleanType(), True),
    StructField("is_active",             BooleanType(), True),
    StructField("risk_tier",             StringType(),  True),
    StructField("total_orders_lifetime", IntegerType(), True),
    StructField("total_spend_lifetime",  DoubleType(),  True),
    StructField("chargeback_count",      IntegerType(), True),
    StructField("referral_source",       StringType(),  True),
    StructField("created_at",            LongType(),    True),
    StructField("last_login_at",         LongType(),    True),
    StructField("ingest_timestamp",      LongType(),    True),
])

DEVICE_SCHEMA = StructType([
    StructField("device_id",        StringType(),  False),
    StructField("user_id",          StringType(),  False),
    StructField("session_id",       StringType(),  True),
    StructField("device_type",      StringType(),  True),
    StructField("os",               StringType(),  True),
    StructField("os_version",       StringType(),  True),
    StructField("browser",          StringType(),  True),
    StructField("browser_version",  StringType(),  True),
    StructField("fingerprint_hash", StringType(),  True),
    StructField("ip_address",       StringType(),  True),
    StructField("vpn_detected",     BooleanType(), True),
    StructField("proxy_detected",   BooleanType(), True),
    StructField("tor_detected",     BooleanType(), True),
    StructField("bot_detected",     BooleanType(), True),
    StructField("screen_resolution",StringType(),  True),
    StructField("timezone",         StringType(),  True),
    StructField("language",         StringType(),  True),
    StructField("created_at",       LongType(),    True),
    StructField("ingest_timestamp", LongType(),    True),
])

GEO_EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),  False),
    StructField("user_id",          StringType(),  False),
    StructField("session_id",       StringType(),  True),
    StructField("ip_address",       StringType(),  True),
    StructField("country_code",     StringType(),  True),
    StructField("country_name",     StringType(),  True),
    StructField("city",             StringType(),  True),
    StructField("region",           StringType(),  True),
    StructField("latitude",         DoubleType(),  True),
    StructField("longitude",        DoubleType(),  True),
    StructField("timezone",         StringType(),  True),
    StructField("isp",              StringType(),  True),
    StructField("is_vpn",           BooleanType(), True),
    StructField("is_proxy",         BooleanType(), True),
    StructField("is_datacenter",    BooleanType(), True),
    StructField("event_type",       StringType(),  True),
    StructField("created_at",       LongType(),    True),
    StructField("ingest_timestamp", LongType(),    True),
])

REFUND_SCHEMA = StructType([
    StructField("refund_id",        StringType(),  False),
    StructField("order_id",         StringType(),  False),
    StructField("payment_id",       StringType(),  False),
    StructField("user_id",          StringType(),  False),
    StructField("amount",           DoubleType(),  False),
    StructField("currency",         StringType(),  True),
    StructField("reason",           StringType(),  True),
    StructField("reason_detail",    StringType(),  True),
    StructField("status",           StringType(),  True),
    StructField("initiated_by",     StringType(),  True),
    StructField("is_fraud_related", BooleanType(), True),
    StructField("created_at",       LongType(),    True),
    StructField("processed_at",     LongType(),    True),
    StructField("ingest_timestamp", LongType(),    True),
])

# Map topic name → schema
TOPIC_SCHEMAS = {
    "orders":     ORDER_SCHEMA,
    "payments":   PAYMENT_SCHEMA,
    "users":      USER_SCHEMA,
    "devices":    DEVICE_SCHEMA,
    "geo_events": GEO_EVENT_SCHEMA,
    "refunds":    REFUND_SCHEMA,
}

"""
Avro schemas for all event types.
Used for Schema Registry registration and validation.
"""

ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "com.fraudplatform.events",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "device_id", "type": "string"},
        {"name": "items", "type": {"type": "array", "items": {
            "type": "record", "name": "OrderItem",
            "fields": [
                {"name": "product_id", "type": "string"},
                {"name": "product_category", "type": "string"},
                {"name": "quantity", "type": "int"},
                {"name": "unit_price", "type": "float"},
            ]
        }}},
        {"name": "total_amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "order_status", "type": "string"},
        {"name": "payment_method", "type": "string"},
        {"name": "shipping_country", "type": "string"},
        {"name": "shipping_city", "type": "string"},
        {"name": "billing_country", "type": "string"},
        {"name": "ip_address", "type": "string"},
        {"name": "user_agent", "type": "string"},
        {"name": "is_fraud", "type": "boolean"},
        {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "ingest_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ]
}

PAYMENT_SCHEMA = {
    "type": "record",
    "name": "Payment",
    "namespace": "com.fraudplatform.events",
    "fields": [
        {"name": "payment_id", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "payment_status", "type": "string"},
        {"name": "payment_method", "type": "string"},
        {"name": "card_last_four", "type": ["null", "string"], "default": None},
        {"name": "card_brand", "type": ["null", "string"], "default": None},
        {"name": "card_country", "type": ["null", "string"], "default": None},
        {"name": "gateway", "type": "string"},
        {"name": "gateway_transaction_id", "type": "string"},
        {"name": "gateway_response_code", "type": "string"},
        {"name": "risk_score", "type": "float"},
        {"name": "is_3ds_authenticated", "type": "boolean"},
        {"name": "processor_fee", "type": "double"},
        {"name": "is_fraud", "type": "boolean"},
        {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "ingest_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ]
}

USER_SCHEMA = {
    "type": "record",
    "name": "User",
    "namespace": "com.fraudplatform.events",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "username", "type": "string"},
        {"name": "first_name", "type": "string"},
        {"name": "last_name", "type": "string"},
        {"name": "country", "type": "string"},
        {"name": "account_age_days", "type": "int"},
        {"name": "is_verified", "type": "boolean"},
        {"name": "is_active", "type": "boolean"},
        {"name": "risk_tier", "type": "string"},
        {"name": "total_orders_lifetime", "type": "int"},
        {"name": "total_spend_lifetime", "type": "double"},
        {"name": "chargeback_count", "type": "int"},
        {"name": "referral_source", "type": "string"},
        {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "last_login_at", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "ingest_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ]
}

DEVICE_SCHEMA = {
    "type": "record",
    "name": "Device",
    "namespace": "com.fraudplatform.events",
    "fields": [
        {"name": "device_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "device_type", "type": "string"},
        {"name": "os", "type": "string"},
        {"name": "os_version", "type": "string"},
        {"name": "browser", "type": "string"},
        {"name": "browser_version", "type": "string"},
        {"name": "fingerprint_hash", "type": "string"},
        {"name": "ip_address", "type": "string"},
        {"name": "vpn_detected", "type": "boolean"},
        {"name": "proxy_detected", "type": "boolean"},
        {"name": "tor_detected", "type": "boolean"},
        {"name": "bot_detected", "type": "boolean"},
        {"name": "screen_resolution", "type": "string"},
        {"name": "timezone", "type": "string"},
        {"name": "language", "type": "string"},
        {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "ingest_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ]
}

GEO_EVENT_SCHEMA = {
    "type": "record",
    "name": "GeoEvent",
    "namespace": "com.fraudplatform.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "session_id", "type": "string"},
        {"name": "ip_address", "type": "string"},
        {"name": "country_code", "type": "string"},
        {"name": "country_name", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "region", "type": "string"},
        {"name": "latitude", "type": "double"},
        {"name": "longitude", "type": "double"},
        {"name": "timezone", "type": "string"},
        {"name": "isp", "type": "string"},
        {"name": "is_vpn", "type": "boolean"},
        {"name": "is_proxy", "type": "boolean"},
        {"name": "is_datacenter", "type": "boolean"},
        {"name": "event_type", "type": "string"},
        {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "ingest_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ]
}

REFUND_SCHEMA = {
    "type": "record",
    "name": "Refund",
    "namespace": "com.fraudplatform.events",
    "fields": [
        {"name": "refund_id", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "payment_id", "type": "string"},
        {"name": "user_id", "type": "string"},
        {"name": "amount", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "reason", "type": "string"},
        {"name": "reason_detail", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "initiated_by", "type": "string"},
        {"name": "is_fraud_related", "type": "boolean"},
        {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"},
        {"name": "processed_at", "type": ["null", "long"], "default": None, "logicalType": "timestamp-millis"},
        {"name": "ingest_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    ]
}

TOPIC_SCHEMAS = {
    "orders": ORDER_SCHEMA,
    "payments": PAYMENT_SCHEMA,
    "users": USER_SCHEMA,
    "devices": DEVICE_SCHEMA,
    "geo_events": GEO_EVENT_SCHEMA,
    "refunds": REFUND_SCHEMA,
}

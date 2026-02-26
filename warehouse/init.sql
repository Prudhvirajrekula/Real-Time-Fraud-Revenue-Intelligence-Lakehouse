-- =============================================================================
-- Fraud Intelligence Platform - PostgreSQL Schema Initialization
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- =============================================================================
-- Schema creation
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS ml_features;

-- =============================================================================
-- GOLD LAYER TABLES (written by Spark gold jobs)
-- =============================================================================

-- Daily Revenue Aggregations
CREATE TABLE IF NOT EXISTS gold.revenue_daily (
    event_date              DATE            NOT NULL,
    shipping_country        VARCHAR(10),
    currency                VARCHAR(10),
    payment_method          VARCHAR(50),
    amount_tier             VARCHAR(20),
    total_orders            BIGINT          DEFAULT 0,
    gmv                     NUMERIC(18, 2)  DEFAULT 0,
    net_revenue             NUMERIC(18, 2)  DEFAULT 0,
    fraud_amount            NUMERIC(18, 2)  DEFAULT 0,
    fraud_orders            BIGINT          DEFAULT 0,
    failed_payments         BIGINT          DEFAULT 0,
    avg_order_value         NUMERIC(12, 2),
    median_order_value      NUMERIC(12, 2),
    p95_order_value         NUMERIC(12, 2),
    unique_customers        BIGINT          DEFAULT 0,
    fraud_rate              NUMERIC(8, 4),
    payment_failure_rate    NUMERIC(8, 4),
    _computed_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_revenue_daily_date    ON gold.revenue_daily (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_revenue_daily_country ON gold.revenue_daily (shipping_country);

-- Hourly Revenue
CREATE TABLE IF NOT EXISTS gold.revenue_hourly (
    event_hour      TIMESTAMP WITH TIME ZONE NOT NULL,
    shipping_country VARCHAR(10),
    order_count     BIGINT          DEFAULT 0,
    hourly_gmv      NUMERIC(18, 2)  DEFAULT 0,
    fraud_count     BIGINT          DEFAULT 0,
    avg_amount      NUMERIC(12, 2),
    fraud_rate      NUMERIC(8, 4),
    _computed_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_revenue_hourly_time ON gold.revenue_hourly (event_hour DESC);

-- Fraud Summary
CREATE TABLE IF NOT EXISTS gold.fraud_summary (
    event_date          DATE            NOT NULL,
    shipping_country    VARCHAR(10),
    payment_method      VARCHAR(50),
    amount_tier         VARCHAR(20),
    total_orders        BIGINT          DEFAULT 0,
    fraud_orders        BIGINT          DEFAULT 0,
    fraud_gmv           NUMERIC(18, 2)  DEFAULT 0,
    total_gmv           NUMERIC(18, 2)  DEFAULT 0,
    avg_fraud_amount    NUMERIC(12, 2),
    geo_mismatch_count  BIGINT          DEFAULT 0,
    vpn_orders          BIGINT          DEFAULT 0,
    fraud_rate          NUMERIC(8, 4),
    fraud_gmv_rate      NUMERIC(8, 4),
    total_refunds       BIGINT          DEFAULT 0,
    total_refund_amount NUMERIC(18, 2)  DEFAULT 0,
    fraud_refunds       BIGINT          DEFAULT 0,
    _computed_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fraud_summary_date    ON gold.fraud_summary (event_date DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_summary_country ON gold.fraud_summary (shipping_country);

-- User Fraud Scores
CREATE TABLE IF NOT EXISTS gold.user_fraud_scores (
    user_id                 VARCHAR(36)     NOT NULL,
    orders_30d              BIGINT          DEFAULT 0,
    fraud_count_30d         BIGINT          DEFAULT 0,
    avg_risk_score          NUMERIC(8, 4),
    avg_velocity_24h        NUMERIC(8, 2),
    vpn_sessions_30d        BIGINT          DEFAULT 0,
    geo_mismatches_30d      BIGINT          DEFAULT 0,
    avg_amount_deviation    NUMERIC(8, 4),
    user_fraud_rate         NUMERIC(8, 4),
    composite_risk_score    NUMERIC(8, 4),
    risk_label              VARCHAR(10),
    _computed_at            TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id)
);

CREATE INDEX IF NOT EXISTS idx_user_fraud_scores_risk ON gold.user_fraud_scores (composite_risk_score DESC);

-- Product Metrics
CREATE TABLE IF NOT EXISTS gold.product_metrics (
    event_date          DATE            NOT NULL,
    category            VARCHAR(100),
    order_count         BIGINT          DEFAULT 0,
    category_revenue    NUMERIC(18, 2)  DEFAULT 0,
    units_sold          BIGINT          DEFAULT 0,
    avg_unit_price      NUMERIC(12, 2),
    fraud_orders        BIGINT          DEFAULT 0,
    _computed_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- ANALYTICS VIEWS
-- =============================================================================

-- 7-day rolling fraud trend
CREATE OR REPLACE VIEW gold.v_fraud_trend_7d AS
SELECT
    event_date,
    SUM(fraud_orders)::NUMERIC / NULLIF(SUM(total_orders), 0) AS global_fraud_rate,
    SUM(fraud_gmv)                                             AS total_fraud_gmv,
    SUM(total_orders)                                          AS total_orders,
    AVG(fraud_rate)                                            AS avg_fraud_rate
FROM gold.fraud_summary
WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY event_date
ORDER BY event_date DESC;

-- Country risk leaderboard
CREATE OR REPLACE VIEW gold.v_country_risk AS
SELECT
    shipping_country,
    AVG(fraud_rate)     AS avg_fraud_rate,
    SUM(fraud_orders)   AS total_fraud_orders,
    SUM(total_orders)   AS total_orders,
    SUM(fraud_gmv)      AS total_fraud_gmv,
    COUNT(DISTINCT event_date) AS days_observed
FROM gold.fraud_summary
WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY shipping_country
ORDER BY avg_fraud_rate DESC;

-- High risk users
CREATE OR REPLACE VIEW gold.v_high_risk_users AS
SELECT *
FROM gold.user_fraud_scores
WHERE composite_risk_score >= 0.50
ORDER BY composite_risk_score DESC;

-- Revenue by payment method (30d)
CREATE OR REPLACE VIEW gold.v_revenue_by_method AS
SELECT
    payment_method,
    SUM(gmv)            AS total_gmv,
    SUM(net_revenue)    AS total_revenue,
    SUM(total_orders)   AS order_count,
    AVG(fraud_rate)     AS avg_fraud_rate,
    AVG(payment_failure_rate) AS avg_failure_rate
FROM gold.revenue_daily
WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY payment_method
ORDER BY total_gmv DESC;

-- =============================================================================
-- PERMISSIONS
-- =============================================================================
GRANT USAGE ON SCHEMA raw, staging, intermediate, marts, gold, silver, ml_features
    TO platform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold      TO platform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver    TO platform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging   TO platform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts     TO platform_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ml_features TO platform_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
    GRANT ALL ON TABLES TO platform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver
    GRANT ALL ON TABLES TO platform_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT ALL ON TABLES TO platform_user;

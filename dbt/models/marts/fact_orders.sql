-- Fact: Orders
-- Core revenue fact table for BI dashboards

{{
  config(
    materialized='view',
    schema='marts'
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
),

enriched as (
    select
        -- Surrogate key
        {{ generate_surrogate_key(['event_date', 'country_code', 'payment_method', 'amount_tier']) }}
                                                                    as order_fact_sk,

        -- Foreign keys
        o.event_date,
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.week_of_year,
        d.day_of_week,
        d.is_weekend,
        d.is_holiday_season,
        d.is_last_7d,
        d.is_last_30d,
        d.is_last_90d,

        -- Dimensions
        o.country_code,
        o.currency,
        o.payment_method,
        o.amount_tier,

        -- Measures
        o.total_orders,
        o.gmv,
        o.net_revenue,
        o.fraud_amount,
        o.fraud_orders,
        o.failed_payments,
        o.avg_order_value,
        o.unique_customers,

        -- Derived KPIs
        o.fraud_rate,
        o.failure_rate,
        round((o.net_revenue / nullif(o.gmv, 0))::numeric, 4)      as net_revenue_margin,
        round((o.fraud_amount / nullif(o.gmv, 0))::numeric, 4)     as fraud_gmv_rate,

        -- Metadata
        o.loaded_at

    from orders o
    left join dates d on o.event_date = d.date_key
)

select * from enriched

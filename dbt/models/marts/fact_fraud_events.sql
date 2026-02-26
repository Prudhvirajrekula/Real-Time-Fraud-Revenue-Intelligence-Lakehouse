-- Fact: Fraud Events
-- Fraud intelligence fact table for risk analytics

{{
  config(
    materialized='view',
    schema='marts'
  )
}}

with fraud as (
    select * from {{ ref('stg_fraud_events') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
),

enriched as (
    select
        -- Surrogate key
        {{ generate_surrogate_key(['event_date', 'country_code', 'payment_method']) }}
                                                                as fraud_fact_sk,
        -- Date FK
        f.event_date,
        d.year,
        d.quarter,
        d.month,
        d.is_weekend,
        d.is_holiday_season,

        -- Dimensions
        f.country_code,
        f.payment_method,
        f.amount_tier,

        -- Fraud Volumes
        f.total_orders,
        f.fraud_orders,
        f.fraud_gmv,
        f.total_gmv,

        -- Fraud Rates
        f.fraud_rate,
        f.fraud_gmv_rate,

        -- Signals
        f.geo_mismatch_count,
        f.vpn_orders,
        f.geo_mismatch_rate,
        f.vpn_rate,

        -- Refunds
        f.refund_count,
        f.refund_amount,
        f.fraud_refund_count,
        round(f.fraud_refund_count::numeric / nullif(f.refund_count, 0), 4)
                                                                as fraud_refund_rate,

        -- Risk flags
        case when f.fraud_rate > 0.10 then true else false end  as is_high_fraud_day,
        case when f.vpn_rate   > 0.15 then true else false end  as is_high_vpn_day,

        f.loaded_at

    from fraud f
    left join dates d on f.event_date = d.date_key
)

select * from enriched

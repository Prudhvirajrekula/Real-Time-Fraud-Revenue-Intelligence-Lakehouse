-- Staging: Fraud Events
-- Source: gold.fraud_summary written by Spark fraud_summary job

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source as (
    select * from {{ source('gold', 'fraud_summary') }}
),

cleaned as (
    select
        event_date,
        shipping_country                              as country_code,
        payment_method,
        amount_tier,

        -- Volume
        total_orders,
        fraud_orders,
        fraud_gmv,
        total_gmv,

        -- Rates
        round(fraud_orders::numeric / nullif(total_orders, 0), 4)   as fraud_rate,
        round(fraud_gmv / nullif(total_gmv, 0.01), 4)               as fraud_gmv_rate,

        -- Signals
        geo_mismatch_count,
        vpn_orders,
        round(geo_mismatch_count::numeric / nullif(total_orders, 0), 4) as geo_mismatch_rate,
        round(vpn_orders::numeric / nullif(total_orders, 0), 4)         as vpn_rate,

        -- Refunds
        coalesce(total_refunds, 0)        as refund_count,
        coalesce(total_refund_amount, 0)  as refund_amount,
        coalesce(fraud_refunds, 0)        as fraud_refund_count,

        _computed_at                      as loaded_at

    from source
    where total_orders > 0
)

select * from cleaned

-- Staging: Orders
-- Source: gold.revenue_daily (written by Spark gold job)
-- Applies type normalization and business naming conventions

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source as (
    select * from {{ source('gold', 'revenue_daily') }}
),

renamed as (
    select
        -- Dimensions
        event_date,
        shipping_country                        as country_code,
        currency,
        payment_method,
        amount_tier,

        -- Measures
        total_orders,
        gmv,
        net_revenue,
        fraud_amount,
        fraud_orders,
        failed_payments,

        -- Derived KPIs
        round(gmv / nullif(total_orders, 0), 2)            as avg_order_value,
        round(fraud_orders::numeric / nullif(total_orders, 0), 4) as fraud_rate,
        round(failed_payments::numeric / nullif(total_orders, 0), 4) as failure_rate,

        unique_customers,
        _computed_at                            as loaded_at

    from source
    where event_date is not null
      and total_orders > 0
)

select * from renamed

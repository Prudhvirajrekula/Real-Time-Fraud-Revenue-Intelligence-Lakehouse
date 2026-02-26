-- Staging: Payments
-- Source: gold.revenue_daily (payment method breakdown)

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source as (
    select * from {{ source('gold', 'revenue_daily') }}
),

payment_summary as (
    select
        event_date,
        payment_method,
        currency,
        shipping_country                         as country_code,

        sum(total_orders)                        as payment_count,
        sum(net_revenue)                         as successful_revenue,
        sum(fraud_amount)                        as fraud_revenue,
        sum(failed_payments)                     as failed_count,

        round(
            sum(failed_payments)::numeric
            / nullif(sum(total_orders), 0), 4
        )                                        as failure_rate,

        round(
            sum(fraud_orders)::numeric
            / nullif(sum(total_orders), 0), 4
        )                                        as fraud_rate,

        round(
            sum(net_revenue) / nullif(sum(total_orders), 0), 2
        )                                        as avg_payment_amount

    from source
    where payment_method is not null
    group by 1, 2, 3, 4
)

select * from payment_summary

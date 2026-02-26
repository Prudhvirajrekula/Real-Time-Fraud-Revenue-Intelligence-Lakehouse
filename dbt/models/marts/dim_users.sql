-- Dimension: Users (SCD Type 1 - overwrite)
-- Enriches user base with risk scores and behavioral segments

{{
  config(
    materialized='view',
    schema='marts'
  )
}}

with user_risk as (
    select * from {{ ref('stg_user_risk') }}
),

user_segments as (
    select
        user_id,
        orders_30d,
        fraud_count_30d,
        avg_risk_score,
        composite_risk_score,
        risk_label,
        is_high_risk,
        vpn_sessions_30d,
        geo_mismatches_30d,

        -- Behavioral segments
        case
            when orders_30d >= 20  then 'power_buyer'
            when orders_30d >= 10  then 'frequent_buyer'
            when orders_30d >= 3   then 'regular_buyer'
            when orders_30d >= 1   then 'occasional_buyer'
            else 'inactive'
        end                                                         as buyer_segment,

        -- Risk band
        case
            when composite_risk_score >= 0.70 then 'critical'
            when composite_risk_score >= 0.50 then 'high'
            when composite_risk_score >= 0.25 then 'elevated'
            when composite_risk_score >= 0.10 then 'moderate'
            else 'low'
        end                                                         as risk_band,

        -- Surrogate key
        {{ generate_surrogate_key(['user_id']) }}                   as user_sk,

        loaded_at

    from user_risk
)

select * from user_segments

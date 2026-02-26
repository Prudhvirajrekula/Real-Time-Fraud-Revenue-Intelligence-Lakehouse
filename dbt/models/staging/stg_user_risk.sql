-- Staging: User Risk Profiles
-- Source: gold.user_fraud_scores written by Spark fraud_summary job

{{
  config(
    materialized='view',
    schema='staging'
  )
}}

with source as (
    select * from {{ source('gold', 'user_fraud_scores') }}
),

enriched as (
    select
        user_id,
        orders_30d,
        fraud_count_30d,
        avg_risk_score,
        avg_velocity_24h,
        vpn_sessions_30d,
        geo_mismatches_30d,
        avg_amount_deviation,
        user_fraud_rate,
        composite_risk_score,
        risk_label,

        -- Flag high-risk users
        case
            when composite_risk_score >= {{ var('fraud_threshold') }} then true
            else false
        end                                             as is_high_risk,

        _computed_at                                    as loaded_at

    from source
)

select * from enriched

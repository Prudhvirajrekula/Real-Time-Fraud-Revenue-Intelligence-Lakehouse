-- Test: Fraud rate must be between 0 and 1
-- Also catches days with suspiciously high fraud (>50%)

select
    event_date,
    country_code,
    fraud_rate,
    'fraud_rate out of bounds [0,1]' as failure_reason
from {{ ref('fact_fraud_events') }}
where fraud_rate < 0
   or fraud_rate > 1

union all

select
    event_date,
    country_code,
    fraud_rate,
    'suspiciously high fraud rate >50%' as failure_reason
from {{ ref('fact_fraud_events') }}
where fraud_rate > 0.5

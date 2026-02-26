-- Test: All GMV values must be non-negative
-- Fails if any row has gmv < 0

select *
from {{ ref('fact_orders') }}
where gmv < 0
   or net_revenue < 0

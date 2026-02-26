-- Dimension: Dates
-- Complete date spine for time-series analytics

{{
  config(
    materialized='view',
    schema='marts'
  )
}}

with date_spine as (
    select generate_series(
        '2023-01-01'::date,
        (current_date + interval '1 year')::date,
        '1 day'::interval
    )::date as date_day
),

enriched as (
    select
        date_day                                                     as date_key,
        date_day                                                     as full_date,
        extract(year  from date_day)::int                           as year,
        extract(quarter from date_day)::int                         as quarter,
        extract(month from date_day)::int                           as month,
        to_char(date_day, 'Month')                                  as month_name,
        to_char(date_day, 'Mon')                                    as month_abbr,
        extract(week from date_day)::int                            as week_of_year,
        extract(day from date_day)::int                             as day_of_month,
        extract(dow from date_day)::int                             as day_of_week,
        to_char(date_day, 'Day')                                    as day_name,
        to_char(date_day, 'YYYY-MM')                               as year_month,
        to_char(date_day, 'IYYY-IW')                               as year_week,
        to_char(date_day, 'YYYY-Q')                                as year_quarter,
        case when extract(dow from date_day) in (0, 6) then true
             else false end                                         as is_weekend,
        case when extract(month from date_day) in (11, 12, 1)
             then true else false end                               as is_holiday_season,
        -- Relative flags
        case when date_day = current_date then true else false end  as is_today,
        case when date_day = current_date - 1 then true else false end as is_yesterday,
        case when date_day >= current_date - 6 then true else false end as is_last_7d,
        case when date_day >= current_date - 29 then true else false end as is_last_30d,
        case when date_day >= current_date - 89 then true else false end as is_last_90d
    from date_spine
)

select * from enriched

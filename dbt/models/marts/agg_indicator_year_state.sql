{{ config(materialized='table') }}

with base as (
  select
    year,
    state,
    state_name,
    (topic || ' - ' || question) as indicator_name,
    value,
    low_ci,
    high_ci
  from {{ ref('stg_cdi_clean') }}
  where value is not null
)
select
  year,
  state,
  max(state_name)                       as state_name,      -- state เดียวกัน ใช้ max() ได้ปลอดภัย
  indicator_name,
  avg(value)                            as avg_value,
  count(*)                              as record_count,
  avg(low_ci)                           as avg_low_ci,
  avg(high_ci)                          as avg_high_ci
from base
group by year, state, indicator_name
order by year, state, indicator_name

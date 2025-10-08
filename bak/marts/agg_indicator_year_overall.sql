{{ config(materialized='table') }}

with base as (
  select
    year,
    (topic || ' - ' || question) as indicator_name,
    value
  from {{ ref('stg_cdi_clean') }}
  where value is not null
)
select
  indicator_name,
  year,
  avg(value)  as avg_value,
  count(*)    as n_obs
from base
group by indicator_name, year
order by indicator_name, year

{{ config(materialized='table') }}

with base as (
  select
    year,
    (topic || ' - ' || question)              as indicator_name,
    coalesce(nullif(strat_cat1,''), 'Overall') as category,
    coalesce(nullif(strat1,''), 'Overall')     as subcategory,
    value
  from {{ ref('stg_cdi_clean') }}
  where value is not null
)
select
  indicator_name,
  category,
  subcategory,
  year,
  avg(value)               as avg_value,
  stddev(value)            as stddev_value,
  count(*)                 as n_obs
from base
group by indicator_name, category, subcategory, year
order by indicator_name, category, subcategory, year

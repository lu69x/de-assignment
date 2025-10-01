{{ config(materialized='table') }}

select
  indicator,
  coalesce(category, 'Overall') as category,
  year,
  avg(value)  as avg_value,
  stddev(value) as stddev_value
from {{ ref('stg_cdi_clean') }}
group by 1,2,3
order by 1,2,3;

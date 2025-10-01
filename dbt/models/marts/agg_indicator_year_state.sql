{{ config(materialized='table') }}

select
  year,
  state,
  indicator,
  avg(value)  as avg_value,
  count(*)    as record_count
from {{ ref('stg_cdi_clean') }}
group by 1,2,3
order by 1,2,3;

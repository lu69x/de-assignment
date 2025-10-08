{{ config(materialized='view') }}

select distinct
  value_type_id,
  value_type,
  unit
from {{ ref('stg_cdi_clean') }}
where value_type_id is not null

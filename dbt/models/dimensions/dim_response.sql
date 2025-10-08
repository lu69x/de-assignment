{{ config(materialized='view') }}

select distinct
  response_id,
  response
from {{ ref('stg_cdi_clean') }}
where response_id is not null

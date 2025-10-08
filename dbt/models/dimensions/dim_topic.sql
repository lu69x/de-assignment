{{ config(materialized='view') }}

select distinct
  topic_id,
  topic
from {{ ref('stg_cdi_clean') }}
where topic_id is not null

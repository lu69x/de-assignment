{{ config(materialized='view') }}

select distinct
  question_id,
  question,
  topic_id
from {{ ref('stg_cdi_clean') }}
where question_id is not null

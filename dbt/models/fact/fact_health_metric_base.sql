
{{ config(materialized='incremental', unique_key='fact_sk') }}

with base as (
  select * from {{ ref('stg_cdi_clean') }}
),
fact as (
  select
    {{ dbt_utils.surrogate_key(['year','year_end','location_id','topic_id','question_id','response_id','value_type_id','state']) }} as fact_sk,
    cast(year as int)      as year_start,
    cast(year_end as int)  as year_end,
    cast(location_id as int) as location_id,
    topic_id,
    question_id,
    response_id,
    value_type_id,
    data_source,
    cast(value as double)  as data_value,
    cast(low_ci as double) as low_confidence_limit,
    cast(high_ci as double) as high_confidence_limit
  from base
  where value is not null
)
select * from fact
{% if is_incremental() %}
-- ถ้ามี timestamp column ควรกรองเฉพาะข้อมูลใหม่/อัพเดท
{% endif %}

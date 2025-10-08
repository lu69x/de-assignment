{{ config(materialized='incremental', unique_key='fact_strat_sk') }}

with s as (
  select * from {{ ref('int_cdi_strata_long') }}
),
fact as (
  select
    {{ dbt_utils.generate_surrogate_key(['obs_sk','strat_cat_id','strat_id']) }} as fact_strat_sk,
    cast(year as int)       as year_start,
    cast(year_end as int)   as year_end,
    cast(location_id as int) as location_id,
    topic_id,
    question_id,
    response_id,
    value_type_id,
    -- stratification (normalized)
    strat_cat_id,
    strat_id,

    -- measures
    cast(value as double)   as data_value,
    cast(low_ci as double)  as low_confidence_limit,
    cast(high_ci as double) as high_confidence_limit,

    -- lineage/debug
    data_source
  from s
  where value is not null
)
select * from fact
{% if is_incremental() %}
-- เพิ่มเงื่อนไข incremental เมื่อมีฟิลด์เวลาจริง
{% endif %}

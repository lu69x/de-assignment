{{ config(materialized='view') }}

with base as (
  select * from {{ ref('stg_cdi_clean') }}
),
-- เตรียมชุด stratification แบบ array แล้ว explode เป็นแถว
unpivoted as (
  select
    {{ dbt_utils.generate_surrogate_key(['year','year_end','location_id','topic_id','question_id','response_id','value_type_id','state']) }} as obs_sk,
    year, year_end,
    location_id, state, state_name, lon, lat,
    topic_id, topic, question_id, question, response_id, response,
    value_type_id, value_type, unit,
    data_source, value, low_ci, high_ci,
    -- สร้าง 3 แถว (หนึ่งแถวต่อช่อง stratification ที่มีค่า)
    v.strat_cat_id, v.strat_cat, v.strat_id, v.strat
  from base
  cross join unnest(array[
    struct_pack(strat_cat_id := strat_cat_id1, strat_cat := strat_cat1, strat_id := strat_id1, strat := strat1),
    struct_pack(strat_cat_id := strat_cat_id2, strat_cat := strat_cat2, strat_id := strat_id2, strat := strat2),
    struct_pack(strat_cat_id := strat_cat_id3, strat_cat := strat_cat3, strat_id := strat_id3, strat := strat3)
  ]) as v(strat_cat_id, strat_cat, strat_id, strat)
  where v.strat_cat_id is not null and v.strat_id is not null
)
select * from unpivoted

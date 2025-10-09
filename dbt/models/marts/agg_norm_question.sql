{{ config(materialized='table') }}

-- -- Base CTE to clean and prepare data
-- with b as (
--   select *
--   from {{ ref('stg_cdi_normalized') }}
--   where value is not null
-- )

select 
  -- ระบุช่วงปี
  b.year as start_year,
  b.year_end as end_year, 

  -- หน่วย/ชนิดค่า และค่าตัวเลขหลัก (normalized)
  b.unit as value_unit,
  b.value_type as value_type,
  b.value as value,  

  -- หมายเหตุ/สัญลักษณ์เชิงอธิบาย
  b.value_footnote_symbol as footnote_symbole,
  b.value_footnote as footnote, 

  -- ช่วงความเชื่อมั่น (normalized)
  b.low_ci as confidence_value_low,
  b.high_ci as confidence_value_high,

  -- -- พิกัด & ไอดีต่าง ๆ
  -- b.location_id as location_id,

  -- ข้อมูลจากตารางมิติคำถาม
  b.question_id as question_id,
  q.question as question_text,
  q.topic_id as topic_id,
  t.topic as topic_text
  
from {{ ref('stg_cdi_normalized') }} as b
left join {{ ref('dim_question') }} as q on q.question_id = b.question_id
left join {{ ref('dim_topic') }} as t on t.topic_id = q.topic_id
-- models/staging/stg_cdi_normalized.sql
select
  -- ระบุช่วงปี
  year,
  year_end,

  -- หน่วย/ชนิดค่า และค่าตัวเลขหลัก (normalized)
  unit,
  value_type,
  value,

  -- หมายเหตุ/สัญลักษณ์เชิงอธิบาย
  value_footnote_symbol,
  value_footnote,

  -- ช่วงความเชื่อมั่น (normalized)
  low_ci,
  high_ci,

  -- พิกัด & ไอดีต่าง ๆ
  location_id,
  question_id,
  response_id,
  value_type_id,

  -- มิติการแบ่งกลุ่ม (Stratification)
  strat_cat_id1,
  strat_id1,
  strat_cat_id2,
  strat_id2,
  strat_cat_id3,
  strat_id3,
  
  strat_cat1,
  strat1,
  strat_cat2,
  strat2,
  strat_cat3,
  strat3,
  
  

from {{ ref('stg_cdi_clean') }}

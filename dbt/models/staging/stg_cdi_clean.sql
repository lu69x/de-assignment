-- models/staging/stg_cdi_clean.sql

with src as (
  select * from {{ ref('stg_cdi_raw') }}
),

-- ฟังก์ชันช่วย: คืนค่า DOUBLE จาก DataValue/DataValueAlt ไม่ว่าจะเป็นเลขจริงหรือสตริงมี % , ฯลฯ
norm as (
  select
    -- 1) พยายามใช้ค่าที่เป็นตัวเลขอยู่แล้ว
    coalesce(
      case when typeof(src.DataValue) in ('DOUBLE','DECIMAL') then cast(src.DataValue as double) end,
      -- 2) ถ้าเป็นสตริง: ลบสัญลักษณ์แล้วแปลง
      try_cast(regexp_replace(cast(src.DataValue as varchar), '%|,', '') as double),
      -- 3) สำรองจาก DataValueAlt (ถ้ามี)
      case when typeof(src.DataValueAlt) in ('DOUBLE','DECIMAL') then cast(src.DataValueAlt as double) end,
      try_cast(regexp_replace(cast(src.DataValueAlt as varchar), '%|,', '') as double)
    ) as value_num,
    -- CI limits อาจถูกอ่านเป็นสตริงเช่นกัน → normalize เหมือนกัน
    coalesce(
      case when typeof(src.LowConfidenceLimit) in ('DOUBLE','DECIMAL') then cast(src.LowConfidenceLimit as double) end,
      try_cast(regexp_replace(cast(src.LowConfidenceLimit as varchar), '%|,', '') as double)
    ) as low_ci_num,
    coalesce(
      case when typeof(src.HighConfidenceLimit) in ('DOUBLE','DECIMAL') then cast(src.HighConfidenceLimit as double) end,
      try_cast(regexp_replace(cast(src.HighConfidenceLimit as varchar), '%|,', '') as double)
    ) as high_ci_num,
    src.*
  from src
)

select
  -- ระบุช่วงปี
  cast(YearStart as integer)  as year,
  cast(YearEnd   as integer)  as year_end,

  -- รัฐ/คำอธิบายพื้นที่
  LocationAbbr                as state,
  LocationDesc                as state_name,

  -- เมทาดาต้า/หัวข้อคำถาม
  DataSource                  as data_source,
  Topic                       as topic,
  Question                    as question,
  nullif(Response, '')        as response,

  -- หน่วย/ชนิดค่า และค่าตัวเลขหลัก (normalized)
  nullif(DataValueUnit, '')   as unit,
  DataValueType               as value_type,
  value_num                   as value,
  /* ถ้าต้องการเก็บค่า alt ที่ normalize แล้วด้วย:
     try_cast(regexp_replace(cast(DataValueAlt as varchar), '%|,', '') as double) as value_alt_norm,
  */

  -- หมายเหตุ/สัญลักษณ์เชิงอธิบาย
  nullif(DataValueFootnoteSymbol, '') as value_footnote_symbol,
  nullif(DataValueFootnote, '')       as value_footnote,

  -- ช่วงความเชื่อมั่น (normalized)
  low_ci_num                   as low_ci,
  high_ci_num                  as high_ci,


  -- พิกัด & ไอดีต่าง ๆ
  -- nullif(Geolocation, '')      as geolocation_wkt,
  nullif(LocationID, '')       as location_id,
  nullif(TopicID, '')          as topic_id,
  nullif(QuestionID, '')       as question_id,
  nullif(ResponseID, '')       as response_id,
  nullif(DataValueTypeID, '')  as value_type_id,

-- มิติการแบ่งกลุ่ม (Stratification)
  StratificationCategory1      as strat_cat1,
  Stratification1              as strat1,
  nullif(StratificationCategory2, '') as strat_cat2,
  nullif(Stratification2, '')         as strat2,
  nullif(StratificationCategory3, '') as strat_cat3,
  nullif(Stratification3, '')         as strat3,
  
  nullif(StratificationCategoryID1, '') as strat_cat_id1,
  nullif(StratificationID1, '')         as strat_id1,
  nullif(StratificationCategoryID2, '') as strat_cat_id2,
  nullif(StratificationID2, '')         as strat_id2,
  nullif(StratificationCategoryID3, '') as strat_cat_id3,
  nullif(StratificationID3, '')         as strat_id3,

  -- ดึง lon/lat จาก "POINT (lon lat)"
  try_cast(regexp_extract(Geolocation, 'POINT \\(([^ ]+) ([^\\)]+)\\)', 1) as double) as lon,
  try_cast(regexp_extract(Geolocation, 'POINT \\(([^ ]+) ([^\\)]+)\\)', 2) as double) as lat

from norm
where value_num is not null

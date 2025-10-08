{{ config(materialized='view') }}

with vals as (
  select strat_cat_id1 as strat_cat_id, strat_id1 as strat_id, strat1 as strat from {{ ref('stg_cdi_clean') }} where strat_id1 is not null
  union
  select strat_cat_id2, strat_id2, strat2 from {{ ref('stg_cdi_clean') }} where strat_id2 is not null
  union
  select strat_cat_id3, strat_id3, strat3 from {{ ref('stg_cdi_clean') }} where strat_id3 is not null
)
select distinct
  strat_cat_id,
  strat_id,
  strat
from vals

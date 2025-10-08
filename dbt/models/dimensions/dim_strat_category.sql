{{ config(materialized='view') }}

with cats as (
  select strat_cat_id1 as strat_cat_id, strat_cat1 as strat_cat from {{ ref('stg_cdi_clean') }} where strat_cat_id1 is not null
  union
  select strat_cat_id2, strat_cat2 from {{ ref('stg_cdi_clean') }} where strat_cat_id2 is not null
  union
  select strat_cat_id3, strat_cat3 from {{ ref('stg_cdi_clean') }} where strat_cat_id3 is not null
)
select distinct strat_cat_id, strat_cat
from cats

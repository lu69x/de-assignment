{{ config(materialized='view') }}

select distinct
  cast(location_id as integer) as location_id,
  state                        as location_abbr,
  state_name                   as location_desc,
  cast(lon as double)          as lon,
  cast(lat as double)          as lat
from {{ ref('stg_cdi_clean') }}
where location_id is not null

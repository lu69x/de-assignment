select
  cast(YearStart as int)                      as year,
  cast(YearEnd as int)                        as year_end,
  LocationAbbr                                as state,
  LocationDesc                                as state_name,
  Indicator                                   as indicator,
  IndicatorCategory                           as indicator_category,
  StratificationCategory1                      as category,
  Stratification1                              as subcategory,
  cast(nullif(DataValue, '') as double)       as value,
  DataValueType                               as value_type,
  DataValueUnit                               as unit
from {{ ref('stg_cdi_raw') }}
where nullif(DataValue, '') is not null;

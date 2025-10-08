{% set csv_path = var('csv_s3_uri', env_var('CSV_S3_URI', 's3://warehouse/assignment/raw/cdc_data.csv')) %}

select *
from read_csv_auto('{{ csv_path }}')

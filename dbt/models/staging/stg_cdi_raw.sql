{% set csv_path = var('csv_s3_uri') %}

select *
from read_csv_auto('{{ csv_path }}')
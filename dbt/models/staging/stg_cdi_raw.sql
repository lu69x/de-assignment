{% set csv_path = var('csv_path') %}

select *
from read_csv_auto('{{ csv_path }}')
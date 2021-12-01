{{
    config(
        materialized='incremental',
        unique_key='clientfiscalperiod_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_clientfiscalperiod_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_clientfiscalperiod_union') }}

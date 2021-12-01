{{
    config(
        materialized='incremental',
        unique_key='clientperiod_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_clientperiod_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_clientperiod_union') }}

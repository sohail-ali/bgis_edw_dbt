{{
    config(
        materialized='incremental',
        unique_key='assetstatus_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_assetstatus_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_assetstatus_union') }}

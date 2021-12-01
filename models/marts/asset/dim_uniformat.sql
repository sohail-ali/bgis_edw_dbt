{{
    config(
        materialized='incremental',
        unique_key='uniformat_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_uniformat_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_uniformat_union') }}

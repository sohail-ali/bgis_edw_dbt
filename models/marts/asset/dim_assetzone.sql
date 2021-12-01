{{
    config(
        materialized='incremental',
        unique_key='assetzone_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_assetzone_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_assetzone_union') }}

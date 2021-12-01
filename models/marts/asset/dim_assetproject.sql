{{
    config(
        materialized='incremental',
        unique_key='assetproject_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_assetproject_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_assetproject_union') }}

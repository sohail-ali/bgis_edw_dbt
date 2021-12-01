{{
    config(
        materialized='incremental',
        unique_key='uom_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_uom_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_uom_union') }}

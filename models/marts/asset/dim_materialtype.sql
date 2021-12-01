{{
    config(
        materialized='incremental',
        unique_key='materialtype_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_materialtype_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_materialtype_union') }}

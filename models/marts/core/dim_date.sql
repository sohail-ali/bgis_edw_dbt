{{
    config(
        materialized='incremental',
        unique_key='date_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_date_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_date_union') }}

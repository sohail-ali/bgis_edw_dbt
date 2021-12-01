{{
    config(
        materialized='incremental',
        unique_key='client_key'
    )
}}

select
  {{ dbt_utils.star(ref('stg_dim_client_union'), except=['_DBT_SOURCE_RELATION']) }}
from {{ ref('stg_dim_client_union') }}


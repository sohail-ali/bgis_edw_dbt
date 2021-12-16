select
  {{ dbt_utils.star(ref('stg_fact_assetcosts_dim_lookups')) }}
from {{ ref('stg_fact_assetcosts_dim_lookups') }}

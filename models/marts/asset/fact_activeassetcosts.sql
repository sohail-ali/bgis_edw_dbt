
select
  {{ dbt_utils.star(ref('stg_fact_activeassetcosts_dim_lookups')) }}
from {{ ref('stg_fact_activeassetcosts_dim_lookups') }}

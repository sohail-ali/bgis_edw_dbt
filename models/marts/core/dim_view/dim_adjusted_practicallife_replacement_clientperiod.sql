select
  {{ dbt_utils.star(from=ref('dim_clientperiod'),prefix='adjusted_practicallife_replacement_') }}
from {{ ref('dim_clientperiod') }}

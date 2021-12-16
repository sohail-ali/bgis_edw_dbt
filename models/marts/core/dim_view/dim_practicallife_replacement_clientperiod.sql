select
  {{ dbt_utils.star(from=ref('dim_clientperiod'),prefix='practicallife_replacement_') }}
from {{ ref('dim_clientperiod') }}

select
  {{ dbt_utils.star(from=ref('dim_clientperiod'),prefix='installation_') }}
from {{ ref('dim_clientperiod') }}

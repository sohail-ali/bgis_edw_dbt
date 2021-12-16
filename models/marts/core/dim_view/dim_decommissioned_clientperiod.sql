select
  {{ dbt_utils.star(from=ref('dim_clientperiod'),prefix='decommissioned_') }}
from {{ ref('dim_clientperiod') }}

select
  {{ dbt_utils.star(from=ref('dim_clientperiod'),prefix='meanservicelife_replacement_') }}
from {{ ref('dim_clientperiod') }}

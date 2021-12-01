select
  {{ dbt_utils.star(source('realsuite', 'lookup_code')) }}
from {{ source('realsuite', 'lookup_code') }}

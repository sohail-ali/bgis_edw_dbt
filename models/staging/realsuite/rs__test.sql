select
  {{ dbt_utils.star(source('realsuite', 'ast_building_value')) }}
from {{ source('realsuite', 'ast_building_value') }}

{%- macro list_columns_name(from, relation_alias=False, except=[]) -%}

{%- set cols = dbt_utils.star(from, relation_alias, except) -%}
{{ cols|replace('\n','')|replace(' ','') }}
{%- endmacro %}
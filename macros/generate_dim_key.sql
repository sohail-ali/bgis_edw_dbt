{% macro generate_dim_key(unique_key, unknown_val=-1) -%}
case when {{ unique_key }} = {{ unknown_val}} then 0 else (ROW_NUMBER() OVER(ORDER BY {{ unique_key }})) -1  end
{%- endmacro %}
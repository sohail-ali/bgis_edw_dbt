{% macro full_month_name(date_col) -%}

    upper(to_char(date({{ date_col }}),'mon-yyyy'))
    
{%- endmacro %}
{% macro cdc_timestamp_col(relation_alias) -%}

    {%- set alias = ""  -%}
    
    {%- if relation_alias|length -%}
        {%- set alias = relation_alias|trim|join(".")  -%}
    {%- endif -%}

    {{ alias }}cdc_modified_datetime as cdc_modified_datetime
    
{%- endmacro %}
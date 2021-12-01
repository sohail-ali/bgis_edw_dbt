{% macro cdc_softdelete_filter(relation_alias) -%}
    {%- set alias = ""  -%}
    
    {%- if relation_alias|length -%}
        {%- set alias = relation_alias|trim|join(".")  -%}
    {%- endif -%}

    case when ifnull( {{ alias }}cdc_softdelete_flag,'') = 'D' then 'Y' else 'N' end = 'N'

{%- endmacro %}
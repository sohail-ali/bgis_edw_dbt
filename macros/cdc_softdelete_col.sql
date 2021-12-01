{% macro cdc_softdelete_col(relation_alias) -%}

    {%- set alias = ""  -%}
    
    {%- if relation_alias|length -%}
        {%- set alias = relation_alias|trim|join(".")  -%}
    {%- endif -%}

    cast(case when ifnull( {{ alias }}cdc_softdelete_flag,'') = 'D' then 'Y' else 'N' end as char(1)) as softdelete_flag
    
{%- endmacro %}
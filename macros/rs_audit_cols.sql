{% macro rs_audit_col(relation_alias,col_type) -%}

    {%- set alias = ""  -%}
    
    {%- if relation_alias|length -%}
        {%- set alias = relation_alias|trim|join(".")  -%}
    {%- endif -%}

    {%- if col_type == 2 -%}

        {{ alias }}created_by as createdby,
        {{ alias }}created_on as created_date,
        {{ alias }}updated_by as updatedby,
        {{ alias }}updated_on as updated_date

    {%- else -%}
        
        {{ alias }}createdby as createdby,
        {{ alias }}createdon as created_date,
        {{ alias }}updatedby as updatedby,
        {{ alias }}updatedon as updated_date

    {%- endif -%}

{%- endmacro %}
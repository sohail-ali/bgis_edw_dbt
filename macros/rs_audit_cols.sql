{%- macro rs_audit_cols(from, relation_alias='') -%}
  {{ return(adapter.dispatch('rs_audit_cols')(from, relation_alias)) }}
{% endmacro %}

{% macro default__rs_audit_cols(from, relation_alias=False) -%}
    {%- do dbt_utils._is_relation(from, 'rs_audit_cols') -%}
    {%- do dbt_utils._is_ephemeral(from, 'rs_audit_cols') -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set createdby_cols = ['createby','created_by','createdby'] %}
    {%- set createdon_cols = ['createdon','created_date','created_on'] %}

    {%- set updatedby_cols = ['updatedby','updated_by'] %}
    {%- set updatedon_cols = ['updatedon','updated_on'] %}
    
    {%- set include_cols = [] %}

    {%- if relation_alias|length -%}
        {%- set relation_alias = relation_alias|trim|join(".")  -%}
    {%- endif -%}


    {%- set cols = adapter.get_columns_in_relation(from) -%}

    {%- for col in cols -%}

        {%- set col_alias = '' -%}

        {%- if col.column|lower in createdby_cols -%}
            {%- set col_alias = relation_alias ~ col.column|lower ~ ' as createdby' -%}
        {%- elif col.column|lower in createdon_cols -%}
            {%- set col_alias = relation_alias ~ col.column|lower ~ ' as created_date' -%}
        {%- elif col.column|lower in updatedby_cols -%}
            {%- set col_alias = relation_alias ~ col.column|lower ~ ' as updatedby' -%}
        {%- elif col.column|lower in updatedon_cols -%}
            {%- set col_alias = relation_alias ~ col.column|lower ~ ' as updated_date' -%}
        {%- endif -%} 

        {%- if col_alias|length -%}
            {% do include_cols.append(col_alias|trim) %}
        {%- endif -%}
    {%- endfor -%}

    {%- for col in include_cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor -%}

{%- endmacro %}


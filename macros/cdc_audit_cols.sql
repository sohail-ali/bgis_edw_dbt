{%- macro cdc_audit_cols(from, relation_alias='') -%}
  {{ return(adapter.dispatch('cdc_audit_cols')(from, relation_alias)) }}
{% endmacro %}

{% macro default__cdc_audit_cols(from, relation_alias=False) -%}
    {%- do dbt_utils._is_relation(from, 'cdc_audit_cols') -%}
    {%- do dbt_utils._is_ephemeral(from, 'cdc_audit_cols') -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set timestamp_col = ['cdc_modified_datetime'] -%}
    {%- set softdelete_col = ['cdc_softdelete_flag'] -%}

    {%- set include_cols = [] -%}

    {%- if relation_alias|length -%}
        {%- set relation_alias = relation_alias|trim|join(".")  -%}
    {%- endif -%}


    {%- set cols = adapter.get_columns_in_relation(from) -%}

    {%- for col in cols -%}

        {%- set col_alias = '' -%}

        {%- if col.column|lower in timestamp_col -%}
            {%- set col_alias = relation_alias ~ col.column|lower ~ ' as cdc_modified_datetime' -%}
        {%- elif col.column|lower in softdelete_col -%}
            {%- set col_alias = "cast(case when ifnull( " ~ relation_alias ~ col.column|lower ~ ",'') ='D' then 'Y' else 'N' end as char(1)) as softdelete_flag" -%}
        {%- endif -%} 

        {%- if col_alias|length -%}
            {%- do include_cols.append(col_alias) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for col in include_cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor -%}

{%- endmacro %}


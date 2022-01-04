{% macro generate_source_model(source_name, table_name) %}

{%- set source_relation = source(source_name, table_name) -%}

{%- set timestamp_col = ['cdc_modified_datetime'] -%}
{%- set softdelete_col = ['cdc_softdelete_flag'] -%}

{%- set createdby_cols = ['createby','created_by','createdby'] %}
{%- set createdon_cols = ['createdon','created_date','created_on'] %}

{%- set updatedby_cols = ['updatedby','updated_by'] %}
{%- set updatedon_cols = ['updatedon','updated_on'] %}
{%- set dss_cols = ['dss_record_source','dss_load_date','dss_create_time','dss_update_time'] %}

{%- set include_cols = [] -%}

{%- set columns = adapter.get_columns_in_relation(source_relation) -%}
{% set column_names=columns | map(attribute='name') %}
{% set base_model_sql %}

{%- for column in column_names %}
    {%- set col_alias = '' -%}

    {%- if column|lower in timestamp_col -%}
        {%- set col_alias = column|lower ~ ' as cdc_modified_datetime' -%}
    {%- elif column|lower in softdelete_col -%}
        {%- set col_alias = "cast(case when ifnull(" ~ column|lower ~ ",'') ='D' then 'Y' else 'N' end as char(1)) as softdelete_flag" -%}
    {%- elif column|lower in createdby_cols -%}
        {%- set col_alias = column|lower ~ ' as createdby' -%}
    {%- elif column|lower in createdon_cols -%}
        {%- set col_alias = column|lower ~ ' as created_date' -%}
    {%- elif column|lower in updatedby_cols -%}
        {%- set col_alias = column|lower ~ ' as updatedby' -%}
    {%- elif column|lower in updatedon_cols -%}
        {%- set col_alias = column|lower ~ ' as updated_date' -%}
    {%- elif column|lower in dss_cols -%}
        {%- set col_alias = '' -%}
    {%- else -%}        
        {%- set col_alias = column|lower ~ ' as ' ~ column|lower -%}
    {%- endif -%}

    {%- if col_alias|length -%}
        {%- do include_cols.append(col_alias) -%}
    {%- endif -%}

{%- endfor -%}

with source as (

    select * from {% raw %}{{ source({% endraw %}'{{ source_name }}', '{{ table_name }}'{% raw %}) }}{% endraw %}

),



renamed as (

    select
        {%- for col in include_cols %}
        {{ col }}{{ "," if not loop.last }}
        {%- endfor %}

    from source

)

select * from renamed
{% endset %}

{% if execute %}

{{ log(base_model_sql, info=True) }}
{% do return(base_model_sql) %}

{% endif %}
{% endmacro %}
{% macro get_table_list(schema_pattern, table_pattern, exclude='', database=target.database) %}
    {{ return(adapter.dispatch('get_table_list')(schema_pattern, table_pattern, exclude, database)) }}
{% endmacro %}

{% macro default__get_table_list(schema_pattern, table_pattern, exclude='', database=target.database) %}

    {%- call statement('get_tables', fetch_result=True) %}

      {{ dbt_utils.get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude, database) }}

    {%- endcall -%}

    {%- set table_list = load_result('get_tables') -%}
    {%- if table_list and table_list['table'] -%}
        {%- set tbl_relations = [] -%}
        {%- for row in table_list['table']  -%}
            {%- set tbl_relation = api.Relation.create(
                database=database,
                schema=row.TABLE_SCHEMA,
                identifier=row.TABLE_NAME,
                type=row.TABLE_TYPE
            ) -%}
            {%- do tbl_relations.append(tbl_relation) -%}
        {%- endfor -%}

        {{ return(tbl_relations) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}

{% endmacro %}
{% macro generate_source_yaml(schema_name, table_pattern, database_name=target.database) %}

{% set sources_yaml=[] %}

{% do sources_yaml.append('version: 2') %}
{% do sources_yaml.append('') %}
{% do sources_yaml.append('sources:') %}
{% do sources_yaml.append('  - name: ' ~ schema_name | lower) %}

{% if database_name != target.database %}
{% do sources_yaml.append('    database: ' ~ database_name | lower) %}
{% endif %}

{% do sources_yaml.append('    tables:') %}


{% set tables=get_table_list(schema_pattern=schema_name,table_pattern=table_pattern, database=database_name) %}

{% for table in tables|sort(attribute='identifier') %}
    {% do sources_yaml.append('      - name: ' ~ table.identifier | replace("PSA_", "") | replace("_ODS_REALSUITE", "") | lower ) %}
    {% do sources_yaml.append('        identifier: ' ~ table.identifier | lower ) %}
    {% do sources_yaml.append('') %}
{% endfor %}

{% if execute %}

    {% set joined = sources_yaml | join ('\n') %}
    {{ log(joined, info=True) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}
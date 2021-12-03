{%- macro surrogate_key() -%}
    {# needed for safe_add to allow for non-keyword arguments see SO post #}
    {# https://stackoverflow.com/questions/13944751/args-kwargs-in-jinja2-macros #}
    {% set frustrating_jinja_feature = varargs %}
    {{ return(adapter.dispatch('surrogate_key')(*varargs)) }}
{% endmacro %}

{%- macro default__surrogate_key() -%}

{% set fields = [] %}

{%- for field in varargs -%}

    {% do fields.append("" ~ field ~ "") %}

{%- endfor -%}

    cast(hash({{ fields|join(',') }}) as number(38))

{%- endmacro -%}
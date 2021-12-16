{%- macro depends_on_dim(from) -%}
    {{ return(adapter.dispatch('depends_on_dim')(from)) }}
{% endmacro %}

{%- macro default__depends_on_dim(from) -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. -#}
    {%- if not execute %}
        {{ return('') }}
    {% endif -%}

    {%- set include_cols = [] %}

    {%- set cols = adapter.get_columns_in_relation(from) -%}

    {%- for col in cols -%}
		
        {%- set include_col = '' -%}
        {%- set dim_tab = '' -%}

        {%- if col.column[-4:]|lower == "_key"  and  col.column[-8:]|lower != "date_key" -%}
            {%- set dim_tab = "dim_" ~ col.column[:-4]|lower -%}
            {%- set include_col = "-- {{ ref('" ~ dim_tab ~ "') }}" ~ "\n" -%}
        {%- endif -%}

        {%- if include_col|length -%}
            {%- do include_cols.append(include_col|trim) -%}
        {%- endif -%}

    {%- endfor %}

    -- depends_on:
    {%- for col in include_cols %}
    {{ col }}
    {%- endfor %} 

{%- endmacro %}
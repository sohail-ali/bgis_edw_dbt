{%- macro unknown_member(from, except=[], override=[]) -%}
    {{ return(adapter.dispatch('unknown_member')(from, except, override)) }}
{% endmacro %}

{%- macro default__unknown_member(from, except=[], override=[]) -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. -#}
    {%- if not execute %}
        {{ return('') }}
    {% endif -%}

    {%- set except = except | map("lower") | list %}

    {%- set include_cols = [] %}
    {%- set cols = adapter.get_columns_in_relation(from) -%}

    {%- for col in cols -%}
		
		{%- set unknown_col = '' -%}
        {%- set ns = namespace(override_col='') -%}

        {%- for x in override %}
            {%- if col.column|lower in x and ns.override_col|length == 0 %}
                {%- set ns.override_col = x -%}
            {%- endif -%}
        {%- endfor %} 
        

        {%- if ns.override_col|length -%}
            {%- set unknown_col = ns.override_col -%}
        {%- elif col.column|lower in except -%}
            {%- set unknown_col = "cast(NULL as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%}
        {%- elif col.column[-4:]|lower == "_key" -%} 
            {%- set unknown_col = "cast(0 as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%}
        {%- elif col.column[-3:]|lower == "_id" and col.is_number() -%}
            {%- set unknown_col = "cast(-1 as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%}
        {%- elif col.column|lower == "softdelete_flag" -%}
            {%- set unknown_col = "cast('N' as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%}
        {%- elif col.is_string() and col.string_size() == 1 -%}
            {%- set unknown_col = "cast('N' as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%}
        {%- elif col.is_string() -%}
            {%- set unknown_col = "cast('N/A' as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%} 
        {%- elif col.is_number() -%}
            {%- set unknown_col = "cast(0 as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%}
        {%- elif "timestamp" in col.data_type|lower or "date" in col.data_type|lower -%}
            {%- set unknown_col = "cast('1900-01-01' as " ~  col.data_type|lower ~ " ) as " ~ col.column|lower -%}
        {%- elif "boolean" in col.data_type|lower -%}
            {%- set unknown_col = "cast(FALSE as " ~  col.data_type|lower ~ " ) as " ~ col.column|lower -%}
        {%- else -%}
            {%- set unknown_col = "cast(NULL as " ~ col.data_type|lower ~ ") as " ~ col.column|lower -%}
        {%- endif -%} 

        {%- if unknown_col|length -%}
            {%- do include_cols.append(unknown_col) -%}
        {%- endif -%}
		
    {%- endfor %}

    select 
    {%- for col in include_cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %} 
    from dual 
{%- endmacro %}
{%- macro join_dim(from, relation_alias='', except=[]) -%}
    {{ return(adapter.dispatch('join_dim')(from, relation_alias, except)) }}
{% endmacro %}

{%- macro default__join_dim(from, relation_alias='', except=[]) -%}

    {#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. -#}
    {%- if not execute %}
        {{ return('') }}
    {% endif -%}

    {%- set except = except | map("lower") | list %}

    {%- if relation_alias|length -%}
        {%- set relation_alias = relation_alias ~ "."  -%}
    {%- endif -%}    

    {%- set include_cols = [] %}
    {%- set join_tabs = [] %}

    {%- set cols = adapter.get_columns_in_relation(from) -%}

    {%- for col in cols -%}
		
        {%- set include_col = '' -%}
        
        {%- set join_tab = '' -%}
        {%- set join_condition = '' -%}
        {%- set dim_tab = '' -%}

        {%- if col.column|lower in except -%}
            {%- set include_col = relation_alias ~ col.column|lower  -%}
        {%- elif col.column[-8:]|lower == "date_key" -%}
            {%- set include_col = relation_alias ~ col.column|lower  -%}
        {%- elif col.column[-4:]|lower == "_key" -%}
            {%- set dim_tab = "dim_" ~ col.column[:-4]|lower -%}
            {%- set ref_dim = ref(dim_tab) -%}
            {%- set include_col = "coalesce(" ~ dim_tab ~ "." ~ col.column|lower ~ ",0) as " ~ col.column|lower -%}
            {%- set join_tab = "left join " ~ ref_dim ~ " as " ~ dim_tab ~ " on" -%}

            {%- set join_condition = relation_alias ~ col.column|lower ~ " = " ~ dim_tab ~ "." ~ col.column|lower -%}
            {%- set join_tab = join_tab ~ "\n\t\t" ~ join_condition -%}

        {%- else -%}
            {%- set include_col = relation_alias ~ col.column|lower  -%}
        {%- endif -%}

        {%- if include_col|length -%}
            {%- do include_cols.append(include_col|trim) -%}
        {%- endif -%}

        {%- if join_tab|length -%}
            {%- do join_tabs.append(join_tab|trim) -%}
        {%- endif -%}

    {%- endfor %}

    select 
    {%- for col in include_cols %}
        {{ col }}{% if not loop.last %},{% endif %}
    {%- endfor %} 
    from {{ from }} as {{ relation_alias[:-1] }}
    {%- for join in join_tabs %}
    {{ join }}
    {%- endfor %} 


{%- endmacro %}
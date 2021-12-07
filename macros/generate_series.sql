{% macro generate_series(upper_bound) %}
    {{ return(adapter.dispatch('generate_series')(upper_bound)) }}
{% endmacro %}

{% macro default__generate_series(upper_bound) %}

    select generated_number from 
    (
    select seq4()+1 as generated_number, uniform(1, 10,1) as constant
    from table(generator(rowcount => {{upper_bound}} )) v 
    order by 1
    ) t

{% endmacro %}
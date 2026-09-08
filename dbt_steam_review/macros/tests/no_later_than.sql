{% macro no_later_than(timestamp_1, timestamp_2) %}
    {{ timestamp_1 }} is not null 
    AND {{ timestamp_2 }} is not null
    AND  {{ timestamp_1 }} > {{timestamp_2}}
    
{% endmacro %}
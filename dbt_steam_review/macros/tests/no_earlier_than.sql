{% macro no_earlier_than(column_name_1, column_name_2) %}
    {{ column_name_1 }} is not null 
    AND {{ column_name_2 }} is not null
    AND  {{ column_name_1 }} < {{column_name_2}}
    
{% endmacro %}
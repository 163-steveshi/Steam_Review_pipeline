{% macro date_not_before(date_column, expected_date) %}
    {{ date_column }} is not null 
    AND  {{ date_column }} < {{ expected_date }}
    
{% endmacro %}
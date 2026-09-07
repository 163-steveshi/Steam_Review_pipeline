{% macro within_range(column_name, min_value, max_value) %}
    {{ column_name }} < {{ min_value }}
    OR {{ column_name }} > {{ max_value }}
{% endmacro %}
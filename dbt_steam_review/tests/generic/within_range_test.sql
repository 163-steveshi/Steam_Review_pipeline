{% test within_range_test(model, column_name, min_value, max_value) %}
    select *
    from {{ model }}
    where {{ within_range(column_name, min_value, max_value) }}
{% endtest %}
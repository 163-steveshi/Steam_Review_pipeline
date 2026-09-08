{% test no_earlier_than_test(model, column_name_1, column_name_2) %}
    select *
    from {{ model }}
    where {{ no_earlier_than(column_name_1, column_name_2) }}
{% endtest %}
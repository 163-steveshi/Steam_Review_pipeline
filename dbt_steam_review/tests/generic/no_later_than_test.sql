{% test no_later_than_test(model, timestamp_1, timestamp_2) %}
    select *
    from {{ model }}
    where {{ no_later_than(timestamp_1, timestamp_2) }}
{% endtest %}
{% test last_play_not_before_review_create_test(model, date_column,app_id_column, checked_app_id, join_model, expected_date, join_key) %}

select *
from {{ model }} as p
inner join {{ join_model }} as r
    on p.{{ join_key }} = r.{{ join_key }}
where {{ 'r.' ~ app_id_column}} = {{ checked_app_id }} and {{ date_not_before(date_column, expected_date)}}

{% endtest %}

{% test app_review_date_not_before_test(model, app_id_column, checked_app_id, date_column, expected_date) %}

select *
from {{ model }}
where {{ app_id_column }} = {{ checked_app_id }} and  {{ date_not_before(date_column, expected_date) }}

{% endtest %}
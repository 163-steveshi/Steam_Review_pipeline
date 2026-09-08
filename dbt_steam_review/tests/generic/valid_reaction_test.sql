{% test valid_reaction_test(model, column_name) %}

with flattened as (

    select
        f.value as element
    from {{ model }},
    lateral flatten(input => {{ column_name }}) f
    where array_size({{ column_name }}) > 0   -- skip empty arrays, they're valid

)

select *
from flattened
where
    -- count must be present and a valid number
    try_to_number(element:count::string) is null
    -- reaction_type must be present and a valid number
    or try_to_number(element:reaction_type::string) is null
    
{% endtest %}
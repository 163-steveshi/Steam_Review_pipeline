
{{ config(materialized='view') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from {{ source('review_scd1', 'DIM_REVIEWS_SCD1') }}
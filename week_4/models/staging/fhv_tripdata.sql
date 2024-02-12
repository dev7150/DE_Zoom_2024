{{
    config(
        materialized='table'
    )
}}


select

    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number
from {{ source('staging','fhv_2019') }}
where EXTRACT(year FROM pickup_datetime) = 2019



-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
{{
    config(
        materialized='table'
    )
}}

with fhv as (
    select *, 
        'fhv' as service_type
    from {{ ref('fhv_tripdata') }})
, 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv.*
from fhv
inner join dim_zones as pickup_zone
on fhv.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv.DOlocationID = dropoff_zone.locationid
where fhv.PUlocationID is not null and fhv.DOlocationID is not null

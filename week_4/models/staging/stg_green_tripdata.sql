{{ config(materialized='table') }}

Select * 

from {{ source("staging","green_tripdata_partitoned_clustered") }}
limit 100
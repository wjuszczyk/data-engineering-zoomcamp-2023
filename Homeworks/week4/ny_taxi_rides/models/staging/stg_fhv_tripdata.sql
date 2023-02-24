{{ config(materialized='view') }}

select
    {{ dbt_utils.surrogate_key(['int64_field_0', 'pickup_datetime']) }} as tripid,
    cast(int64_field_0 as integer) as old_index,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationID as numeric) as pulocationid,
    cast(DOlocationID as numeric) as dolocationid,
    cast(SR_Flag as numeric) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number,
from {{ source('staging', 'fhv_2019') }}

{{ config(materialized="table") }}

with trips_data as (select * from {{ ref("fhv_fact_trips") }})
select
    date_trunc(pickup_datetime, month) as month, count(tripid) as total_monthly_trips,
from trips_data
group by 1
order by 2 desc
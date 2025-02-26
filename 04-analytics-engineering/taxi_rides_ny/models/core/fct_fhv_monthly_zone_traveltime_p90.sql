{{
    config(
        materialized='table'
    )
}}

with trip_data as (
    select 
        *,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
),
p90_trip_duration as (
    select 
        year,
        month,
        pickup_borough,
        pickup_zone,
        dropoff_borough,
        dropoff_zone,
        percentile_cont(trip_duration, 0.90) over (
            partition by year, month, pickup_locationid, dropoff_locationid
        ) as trip_duration_p90
    from trip_data
)
select distinct * from p90_trip_duration

{{
    config(
        materialized='table'
    )
}}

with fact_trips_data as (
  select 
    service_type, 
    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month,
    fare_amount
  from {{ ref('fact_trips') }}
  where fare_amount > 0 
    and trip_distance > 0 
    and payment_type_description in ('Cash', 'Credit Card')
)

SELECT 
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (
        PARTITION BY service_type, year, month
    ) AS p97
FROM fact_trips_data;


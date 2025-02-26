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
    and payment_type_description in ('Cash', 'Credit card')
),

fact_trips_data_percent AS (
SELECT 
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (
        PARTITION BY service_type, year, month
    ) AS p97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (
        PARTITION BY service_type, year, month
    ) AS p95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (
        PARTITION BY service_type, year, month
    ) AS p90
FROM fact_trips_data
)

SELECT 
    service_type,
    year,
    month,
    ANY_VALUE(p97) AS p97,
    ANY_VALUE(p95) AS p95,
    ANY_VALUE(p90) AS p90
FROM fact_trips_data_percent
group by service_type, year, month

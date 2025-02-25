{{
    config(
        materialized='table'
    )
}}

with trips_with_quarter as (
    select 
        tripid, 
        total_amount, 
        service_type,  -- Include service_type
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter
    from {{ ref('fact_trips') }}
),

-- Aggregate revenue by year, quarter, and service_type
quarterly_revenue as (
    select
        year,
        quarter,
        service_type,  -- Include service_type in grouping
        sum(total_amount) as total_revenue
    from trips_with_quarter
    group by year, quarter, service_type
),

-- Calculate YoY growth by service_type
quarterly_yoy_growth as (
    select 
        curr.year, 
        curr.quarter, 
        curr.service_type, 
        curr.total_revenue,
        lag(curr.total_revenue) over (
            partition by curr.service_type, curr.quarter 
            order by curr.year
        ) as prev_total_revenue
    from quarterly_revenue as curr
)
select 
    year,
    quarter,
    service_type,
    total_revenue,
    prev_total_revenue,
    case 
        when prev_total_revenue is null or prev_total_revenue = 0 then 0
        else (total_revenue - prev_total_revenue) / prev_total_revenue
    end as yoy_growth
from quarterly_yoy_growth
order by service_type, year, quarter
with day_agg as (
  select
    requested_city,
    date_trunc('day', observed_utc) as day,
    avg(pm25) as pm25_avg
  from {{ ref('stg_measurements_hourly') }}
  group by 1,2
)
select * from day_agg;

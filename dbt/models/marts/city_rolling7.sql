select
  requested_city,
  day,
  pm25_avg,
  avg(pm25_avg) over (
    partition by requested_city
    order by day
    -- Calendar-based window: "6 rows preceding" would span far more than a week
    -- whenever a city is missing days of data.
    range between interval '6 days' preceding and current row
  ) as pm25_rolling7
from {{ ref('city_daily_avg') }}

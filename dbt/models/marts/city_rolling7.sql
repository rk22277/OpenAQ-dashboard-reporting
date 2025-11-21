select
  requested_city,
  day,
  pm25_avg,
  avg(pm25_avg) over (
    partition by requested_city
    order by day
    rows between 6 preceding and current row
  ) as pm25_rolling7
from {{ ref('city_daily_avg') }};

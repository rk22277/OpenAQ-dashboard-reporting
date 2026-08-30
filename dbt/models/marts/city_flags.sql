select
  requested_city,
  day,
  pm25_avg,
  pm25_rolling7,
  -- WHO 2021 Air Quality Guideline: 24-hour mean PM2.5 should not exceed 15 µg/m³.
  case when pm25_avg > 15 then '⚠️ Above WHO 24h guideline' else '✅ Within guideline' end as status
from {{ ref('city_rolling7') }}

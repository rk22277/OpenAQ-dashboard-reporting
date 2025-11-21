select
  requested_city,
  day,
  pm25_avg,
  pm25_rolling7,
  case when pm25_avg > 25 then '⚠️ Above WHO limit' else '✅ Safe' end as status
from {{ ref('city_rolling7') }};

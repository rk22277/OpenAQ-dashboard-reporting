select
  requested_city,
  observed_utc,
  parameter_name,
  value::float as pm25,
  units,
  locality,
  pulled_at
from core.measurements_hourly
where parameter_name = 'pm25'
  and value is not null
  -- OpenAQ occasionally emits negative sentinel values for missing readings;
  -- keep them out so daily/rolling averages are not dragged below zero.
  and value >= 0

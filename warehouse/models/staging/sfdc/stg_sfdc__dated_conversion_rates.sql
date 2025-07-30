WITH dated_conversion_rates AS (
  SELECT *
  FROM {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
                  {{ ref('base_fed_sfdc__dated_conversion_rate') }} 
                {% else %} 
                  {{ source('salesforce', 'dated_conversion_rate') }}
                {% endif %}
)

select 
  id,
  iso_code,
  conversion_rate,
  start_date::date as start_date,
  next_start_date::date as next_start_date,
  to_timestamp_ntz(created_date) as created_at,
  to_timestamp_ntz(last_modified_date) as last_modified_date,
  row_number() over(
      partition by iso_code
      order by start_date desc
    ) as conversion_rate_period_sequence

from dated_conversion_rates

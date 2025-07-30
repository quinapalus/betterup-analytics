WITH usd_exchange_rates AS (

  SELECT * FROM {{ source('gsheets_coach_payments', 'usd_exchange_rates') }}

)

SELECT
  {{ dbt_utils.surrogate_key(['month_key', 'currency']) }} as primary_key,
  TO_CHAR(month_key::INT) AS month_key,
  LEFT(ROUND(month_key)::STRING, 4) || '-' || RIGHT(ROUND(month_key)::STRING, 2) AS month_key_formatted,
  currency,
  exchange_rate,
  gross_exchange_rate,
  markup_rate
FROM usd_exchange_rates

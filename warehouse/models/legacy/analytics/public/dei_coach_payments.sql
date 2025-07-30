WITH coach_payments_local_currency AS (

  SELECT * FROM {{ref('dbt_coach_payments_local_currency')}}

),

usd_exchange_rates AS (

  SELECT * FROM {{(ref('stg_gsheets__usd_exchange_rates'))}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}}

),

organizations AS (

  SELECT * FROM {{ref('stg_app__organizations')}}

)


SELECT
  lc.shl_payment_id,
  lc.billable_event_id,
  lc.coach_id,
  lc.member_id,
  lc.track_id,
  t.organization_id,
  o.sfdc_account_id,
  t.contract_id,
  lc.event_type,
  lc.augmented_event_type,
  lc.is_coach_cost_manual_upload,
  t.deployment_type,
  lc.event_at,
  lc.event_reported_at,
  lc.session_requested_length,
  lc.usage_minutes,
  lc.sent_to_processor_at,
  lc.shl_payment_local_amount,
  lc.shl_payment_local_currency,
  ROUND((lc.shl_payment_local_amount * er.exchange_rate::FLOAT), 2) AS payment_without_markup_usd_amount,
  ROUND(((lc.shl_payment_local_amount * er.exchange_rate::FLOAT) * er.markup_rate::FLOAT), 2) AS markup_usd_amount,
  ROUND((lc.shl_payment_local_amount * er.gross_exchange_rate::FLOAT), 2) AS payment_with_markup_usd_amount
FROM coach_payments_local_currency AS lc
LEFT OUTER JOIN usd_exchange_rates AS er
  ON {{ month_key ('lc.event_reported_at')}} = er.month_key
  AND lc.shl_payment_local_currency = er.currency
  -- specify NOT NULL condition because the base column can have NULL values when joining.
LEFT OUTER JOIN tracks AS t
  ON (lc.track_id IS NOT NULL AND lc.track_id = t.track_id)
LEFT OUTER JOIN organizations AS o
  ON (t.organization_id IS NOT NULL AND t.organization_id = o.organization_id)

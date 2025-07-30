WITH coach_payments AS (

  SELECT * FROM {{ref('dei_coach_payments')}}

),

event_type_accounting_categories AS (

  SELECT * FROM {{ref('stg_gsheets_deployments__bu_event_type_accounting_categories')}}

),

coaches AS (

  SELECT * FROM {{ref('dbt_coach')}}

)


SELECT
  cp.shl_payment_id,
  {{ date_key ('cp.event_reported_at') }} AS date_key,
  -- use COALESCE since some coaches might not be in dim_coach
  COALESCE(c.coach_key, {{ coach_key ('c.fnt_applicant_id', 'c.app_coach_id') }}) AS coach_key,
  {{ member_key('cp.member_id') }} AS member_key,
  {{ deployment_key('cp.track_id') }} AS deployment_key,
  {{ account_key('cp.organization_id', 'cp.sfdc_account_id') }} AS account_key,
  {{ contract_key('cp.contract_id') }} AS contract_key,
  cp.billable_event_id AS app_billable_event_id,
  cp.event_type AS app_event_type,
  cp.augmented_event_type,
  cp.is_coach_cost_manual_upload,
  cp.deployment_type,
  cp.event_at AS app_event_at,
  cp.event_reported_at,
  cp.session_requested_length,
  cp.usage_minutes,
  cp.shl_payment_local_amount AS coach_payment_local_amount,
  cp.shl_payment_local_currency AS coach_payment_local_currency,
  cp.payment_without_markup_usd_amount AS coach_payment_without_markup_usd_amount,
  cp.markup_usd_amount AS coach_markup_usd_amount,
  cp.payment_with_markup_usd_amount AS coach_payment_with_markup_usd_amount,
  ec.accounting_category,
  ec.parent_accounting_category
FROM coach_payments AS cp
LEFT OUTER JOIN coaches AS c
  ON cp.coach_id = c.app_coach_id
LEFT OUTER JOIN event_type_accounting_categories AS ec
  ON cp.deployment_type = ec.deployment_type
  AND cp.event_type = ec.event_type

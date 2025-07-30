{{
  config(
    tags=["eu"]
  )
}}

with billable_events as (

  -- pass through legacy base model until we refactor project to
  -- pull directly from source in the staging layer
  select
      billable_event_id,
      event_type,
      event_at,
      payment_status,
      usage_minutes,
      units,
      amount_due,
      currency_code,
      member_id,
      coach_id,
      coaching_cloud,
      track_id,
      track_assignment_id,
      product_subscription_assignment_id,
      associated_record_type,
      coaching_type,
      associated_record_id,
      has_pay_rate,
      response_body,
      payment_id,
      notes,
      coach_profile_pay_rate_id,
      {{ load_timestamp('sent_to_processor_at') }},
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}

  from {{ ref('app_billable_events') }}
  /*
  The archived records are records that have been
  deleted in source db and lost due to ingestion re-replication.

  A large scale re-replication occured in 2023-06 during the Stitch upgrade
  and the creation of the new landing schema - stitch_app_v2.
  The app_archive tables found with a tag 2023_06 hold the records
  that pertain to the deleted records at that time and reference can be found in
  ../models/staging/app/sources_schema_app.yml file.

  Details of the upgrade process & postmortem can be found in the Confluence doc titled:
  "stitch_app_v2 upgrade | Process Reference Doc"
  https://betterup.atlassian.net/wiki/spaces/DATA/pages/3418750982/stitch+app+v2+upgrade+Process+Reference+Doc

*/
  {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
  union
  select
      id as billable_event_id,
      CASE
        WHEN event_type = 'missed_appointment' THEN 'missed_session'
        ELSE event_type
      END AS event_type,
      {{ load_timestamp('event_at') }},
      payment_status,
      usage_minutes,
      units,
      amount_due,
      currency_code,
      user_id as member_id,
      coach_id,
      coaching_cloud,
      track_id,
      track_assignment_id,
      product_subscription_assignment_id,
      CASE
        WHEN associated_record_type = 'Appointment' THEN 'Session'
        ELSE associated_record_type
      END AS associated_record_type,
      coaching_type,
      associated_record_id,
      coach_profile_pay_rate_id IS NOT NULL AS has_pay_rate,
      PARSE_JSON(response_body) AS response_body,
      payment_id,
      notes,
      coach_profile_pay_rate_id,
      {{ load_timestamp('sent_to_processor_at') }},
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__billable_events_historical') }}
  {% endif -%}

),

annual_usd_exchange_rates as (
    select * from {{ source('gsheets_coach_payments', 'fixed_annual_usd_exchange_rates') }}
),

billable_amount as (
  select
    be.billable_event_id,
    be.amount_due,
    be.currency_code,
    case
        when date_part('year',be.event_at) between 2000 and 2016
            then 2017 --we don't have annual rates FROM before 2017. For these billable events will use 2017 rates.
        when date_part('year',be.event_at) < 2000
            then date_part('year',be.created_at) --there are some records with a event_at year value of 6, 7, 10 etc. In these cases we will use year of created_at instead.
        else date_part('year',be.event_at) end as billable_event_year
  from billable_events as be
),

billable_amount_usd_conversion as (
  select
    ba.*,
    rates.fixed_annual_fx_rate,
    ba.amount_due * (case when ba.currency_code = 'PAB' --We don't have Panamanian Balboa in our rates table but it is pegged to US Dollar.
                              then 1.00 ELSE rates.fixed_annual_fx_rate end) as amount_due_usd
    from billable_amount as ba
     left join annual_usd_exchange_rates rates
    on rates.payment_local_currency = ba.currency_code
       and rates.year = ba.billable_event_year
    where ba.amount_due is not null
)


select
  be.*,
  billable_amount_usd_conversion.amount_due_usd,
  billable_amount_usd_conversion.fixed_annual_fx_rate
from billable_events as be
left join billable_amount_usd_conversion
  on billable_amount_usd_conversion.billable_event_id = be.billable_event_id

{{
  config(
    tags=["eu"],
    materialized='table'
  )
}}

WITH current_billable_events AS (

  SELECT
    amount_due,
    associated_record_id,
    associated_record_type,
    coaching_cloud,
    coaching_type,
    coach_id,
    coach_profile_pay_rate_id,
    created_at,
    currency_code,
    event_at,
    event_type,
    id,
    notes,
    payment_id,
    payment_status,
    processor_success,
    product_subscription_assignment_id,
    reference_id,
    request_body::varchar as request_body,
    response_body::varchar as response_body,
    sent_to_processor_at,
    shortlist_workitem_id,
    track_assignment_id,
    track_id,
    units,
    updated_at,
    usage_minutes,
    user_id,
    coach_profile_pay_rate_uuid

    FROM {{ source('app', 'billable_events') }}
    WHERE id NOT IN (
    -- do not include billable events that were
    -- removed in the Admin Panel
    SELECT
      item_id
    FROM {{ref('stg_app__versions_delete')}}
    WHERE item_type = 'BillableEvent'
  )
),
{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}

  archived_billable_events as (
/*

  The archived records in this CTE are records that have been
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

select
    amount_due,
    associated_record_id,
    associated_record_type,
    coaching_cloud,
    coaching_type,
    coach_id,
    coach_profile_pay_rate_id,
    created_at,
    currency_code,
    event_at,
    event_type,
    id,
    notes,
    payment_id,
    payment_status,
    processor_success,
    product_subscription_assignment_id,
    reference_id,
    request_body::varchar as request_body,
    response_body::varchar as response_body,
    sent_to_processor_at,
    shortlist_workitem_id,
    track_assignment_id,
    track_id,
    units,
    updated_at,
    usage_minutes,
    user_id,
    coach_profile_pay_rate_uuid
  from {{ ref('base_app__billable_events_historical') }}
),
{% endif -%}

final as (

  select * from current_billable_events
  {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
  union
  select * from archived_billable_events
  where id not in (select id from current_billable_events)
  {% endif -%}

)

SELECT
  be.id AS billable_event_id,
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
  user_id AS member_id,
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
FROM final AS be

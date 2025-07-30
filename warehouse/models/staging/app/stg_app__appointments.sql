{{
  config(
    tags=["eu"]
  )
}}

WITH src_appointments AS (
  SELECT
    id
    , uuid
    , created_at
    , updated_at
    , timeslot_id
    , coach_id
    , creator_id
    , user_id
    , starts_at
    , ends_at
    , started_at
    , complete_at
    , canceled_at
    , call_id
    , participant_id
    , canceled_by_user_id
    , coach_assignment_id
    , confirmed_at
    , contact_method
    , length
    , missed
    , recurring
    , original_starts_at
    , development_topic_id
    , post_session_member_assessment_id
    , requested_length
    , requested_weekly_interval
    , track_assignment_id
    , nylas_event_id
    , product_subscription_assignment_id
    , last_rescheduled_by_user_id
    , token
    , sequence_number

  FROM {{ source('app', 'appointments') }}
),

current_appointments as (

  select
     id AS appointment_id
    , uuid
    , created_at::timestamp_ntz AS appointment_created_at
    , updated_at::timestamp_ntz AS appointment_updated_at
    , timeslot_id
    , coach_id
    , creator_id
    , user_id AS member_id
    , starts_at::timestamp_ntz AS starts_at
    , ends_at::timestamp_ntz AS ends_at
    , started_at::timestamp_ntz AS started_at
    , complete_at::timestamp_ntz AS complete_at
    , canceled_at::timestamp_ntz AS canceled_at
    , complete_at IS NOT NULL AS is_completed --flag for completed sessions
    , call_id
    , participant_id
    , canceled_by_user_id
    , coach_assignment_id
    , confirmed_at::timestamp_ntz AS confirmed_at
    , contact_method
    , length AS appointment_length
    , missed AS is_appointment_missed
    , recurring AS is_appointment_recurring
    , original_starts_at
    , development_topic_id
    , post_session_member_assessment_id
    , requested_length
    , requested_weekly_interval
    , track_assignment_id
    , nylas_event_id
    , product_subscription_assignment_id
    , last_rescheduled_by_user_id
    , token
    , sequence_number

    --window functions for downstream help
    , row_number() over (partition by member_id, coach_id order by appointment_created_at asc) as member_coach_session_order

    --only count sessions that are completed to get the order of completed sessions
    , iff(complete_at is not null, row_number() over (partition by member_id,
                                                      coach_id order by complete_at asc), null) as member_coach_session_completed_order

    --booleans to track whether or not a user returns to a given coach to attend multiple sessions
    , iff(member_coach_session_order = 1, true, false) as is_first_member_coach_session
    , iff(member_coach_session_order = 2, true, false) as is_second_member_coach_session
    , iff(member_coach_session_order = 3, true, false) as is_third_member_coach_session
  from src_appointments

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}

, archived_appointments as (
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
     id AS appointment_id
    , uuid
    , created_at::timestamp_ntz AS appointment_created_at
    , updated_at::timestamp_ntz AS appointment_updated_at
    , timeslot_id
    , coach_id
    , creator_id
    , user_id AS member_id
    , starts_at::timestamp_ntz AS starts_at
    , ends_at::timestamp_ntz AS ends_at
    , started_at::timestamp_ntz AS started_at
    , complete_at::timestamp_ntz AS complete_at
    , canceled_at::timestamp_ntz AS canceled_at
    , complete_at IS NOT NULL AS is_completed --flag for completed sessions
    , call_id
    , participant_id
    , canceled_by_user_id
    , coach_assignment_id
    , confirmed_at::timestamp_ntz AS confirmed_at
    , contact_method
    , length AS appointment_length
    , missed AS is_appointment_missed
    , recurring AS is_appointment_recurring
    , original_starts_at
    , development_topic_id
    , post_session_member_assessment_id
    , requested_length
    , requested_weekly_interval
    , track_assignment_id
    , nylas_event_id
    , product_subscription_assignment_id
    , last_rescheduled_by_user_id
    , token
    , sequence_number

    --window functions for downstream help
    , row_number() over (partition by member_id, coach_id order by appointment_created_at asc) as member_coach_session_order

    --only count sessions that are completed to get the order of completed sessions
    , iff(complete_at is not null, row_number() over (partition by member_id,
                                                      coach_id order by complete_at asc), null) as member_coach_session_completed_order

    --booleans to track whether or not a user returns to a given coach to attend multiple sessions
    , iff(member_coach_session_order = 1, true, false) as is_first_member_coach_session
    , iff(member_coach_session_order = 2, true, false) as is_second_member_coach_session
    , iff(member_coach_session_order = 3, true, false) as is_third_member_coach_session
  from {{ ref('base_app__appointments_historical') }}
)


select * from archived_appointments
union
{% endif -%}
select * from current_appointments


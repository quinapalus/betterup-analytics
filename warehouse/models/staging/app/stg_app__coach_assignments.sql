{{
  config(
    tags=["eu"]
  )
}}

WITH coach_assignments AS (

  select * from {{ source('app', 'coach_assignments') }}

),

current_coach_assignments as (

  select
      id as coach_assignment_id,
      coach_id,
      coach_recommendation_id,
      created_at::timestamp_ntz as created_at,
      ended_at::timestamp_ntz as ended_at,
      ended_reason,
      last_appointment_id,
      next_appointment_id,
      role,
      specialist_vertical_uuid,
      specialist_vertical_id, --still functional but only for US data
      updated_at::timestamp_ntz as updated_at,
      user_id as member_id,
      user_unread_message_count,
      member_last_active_at,
      coach_unread_message_count,
      last_message_sent_at,
      last_message_id,
      next_appointment_at,
      group_coaching_registration_id,
      debrief360
  from coach_assignments

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_coach_assignments as (
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
    id as coach_assignment_id,
    coach_id,
    coach_recommendation_id,
    created_at::timestamp_ntz as created_at,
    ended_at::timestamp_ntz as ended_at,
    ended_reason,
    last_appointment_id,
    next_appointment_id,
    role,
    specialist_vertical_uuid,
    specialist_vertical_id, --still functional but only for US data
    updated_at::timestamp_ntz as updated_at,
    user_id as member_id,
    user_unread_message_count,
    member_last_active_at,
    coach_unread_message_count,
    last_message_sent_at,
    last_message_id,
    next_appointment_at,
    group_coaching_registration_id,
    debrief360
  from {{ ref('base_app__coach_assignments_historical') }}
)


select * from archived_coach_assignments
union
{% endif -%}
select * from current_coach_assignments

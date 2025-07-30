{{
  config(
    tags=["eu"]
  )
}}

WITH messages AS (

  select * from {{ source('app', 'messages') }}

),

current_messages as (

  select
        id AS message_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        attachment_status,
        body,
        client_uuid,
        coach_assignment_id,
        created_from,
        {{ load_timestamp('read_by_recipient_at') }},
        recipient_id,
        sender_id,
        conversation_participant_id,
        generated_message_id
  from messages

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_messages as (
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
        id AS message_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        attachment_status,
        body,
        client_uuid,
        coach_assignment_id,
        created_from,
        {{ load_timestamp('read_by_recipient_at') }},
        recipient_id,
        sender_id,
        conversation_participant_id,
        generated_message_id
  from {{ ref('base_app__messages_historical') }}
)


select * from archived_messages
union
{% endif -%}
select * from current_messages

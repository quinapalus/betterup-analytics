WITH conversations AS (

  select * from {{ source('app', 'conversations') }}

),

current_conversations as (

  select
      id AS conversation_id,
      assigned_care_guide_id,
      conversation_subject_id,
      care_guide_responded,
      case_notes,
      next_steps,
      participants_count,
      priority,
      status,
      type,
      {{ load_timestamp('due_at') }},
      {{ load_timestamp('closed_at') }},
      {{ load_timestamp('last_member_message_at') }},
      {{ load_timestamp('last_message_at') }},
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from conversations

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_conversations as (
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
      id AS conversation_id,
      assigned_care_guide_id,
      conversation_subject_id,
      care_guide_responded,
      case_notes,
      next_steps,
      participants_count,
      priority,
      status,
      type,
      {{ load_timestamp('due_at') }},
      {{ load_timestamp('closed_at') }},
      {{ load_timestamp('last_member_message_at') }},
      {{ load_timestamp('last_message_at') }},
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__conversations_historical') }}
)


select * from archived_conversations
union
{% endif -%}
select * from current_conversations

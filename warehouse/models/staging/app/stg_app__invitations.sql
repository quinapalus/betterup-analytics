WITH invitations AS (

  select * from {{ source('app', 'invitations') }}

),

current_invitations as (

  select
        id as invitation_id,
        associated_object_id,
        invited_user_id,
        invite_track_id,
        inviting_user_id,
        invitee_email,
        type,
        language,
        gclid,
        utm_params,
        associated_object_type,
        {{ load_timestamp('accepted_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from invitations

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_invitations as (
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
        id as invitation_id,
        associated_object_id,
        invited_user_id,
        invite_track_id,
        inviting_user_id,
        invitee_email,
        type,
        language,
        gclid,
        utm_params,
        associated_object_type,
        {{ load_timestamp('accepted_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__invitations_historical') }}
)


select * from archived_invitations
union
{% endif -%}
select * from current_invitations

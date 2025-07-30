WITH comms_notifications AS (

  select * from {{ source('app', 'comms_notifications') }}

),

current_comms_notifications as (

  select
        id as comms_notification_id,
        context,
        expires_at,
        subject,
        uuid,
        recipient_uuid,
        params,
        notification_type,
        digest,
        only_deliver_once,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from comms_notifications

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_comms_notifications as (
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
        id as comms_notification_id,
        context,
        expires_at,
        subject,
        uuid,
        recipient_uuid,
        params,
        notification_type,
        digest,
        only_deliver_once,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__comms_notifications_historical') }}
)


select * from archived_comms_notifications
union
{% endif -%}
select * from current_comms_notifications

{{
  config(
    tags=['classification.c3_confidential']
  )
}}

with active_storage_attachments as (

    select * from {{ source('app', 'active_storage_attachments') }}

),

current_active_storage_attachments as (

  select
      id as active_storage_attachment_id,
      name,
      record_type,
      record_id,
      {{ load_timestamp('created_at') }}
  from active_storage_attachments

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_active_storage_attachments as (
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
        id as active_storage_attachment_id,
        name,
        record_type,
        record_id,
        {{ load_timestamp('created_at') }}
  from {{ ref('base_app__active_storage_attachments_historical') }}
)


select * from archived_active_storage_attachments
union
{% endif -%}
select * from current_active_storage_attachments


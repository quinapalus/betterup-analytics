WITH milestones AS (

  select * from {{ source('app', 'milestones') }}

),

current_milestones as (

  select
        id as milestone_id,
        user_id as member_id,
        associated_record_id,
        associated_record_type,
        context,
        locked,
        type,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('completed_at') }},
        {{ load_timestamp('updated_at') }}
  from milestones

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_milestones as (
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
        id as milestone_id,
        user_id as member_id,
        associated_record_id,
        associated_record_type,
        context,
        locked,
        type,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('completed_at') }},
        {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__milestones_historical') }}
)


select * from archived_milestones
union
{% endif -%}
select * from current_milestones

WITH user_groups AS (

  select * from {{ source('app', 'user_groups') }}

),

current_user_groups as (

  select
        id as user_group_id,
        user_id as member_id,
        group_id,
        role,
        migration_version,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from user_groups

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_user_groups as (
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
        id as user_group_id,
        user_id as member_id,
        group_id,
        role,
        migration_version,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__user_groups_historical') }}
)


select * from archived_user_groups
union
{% endif -%}
select * from current_user_groups

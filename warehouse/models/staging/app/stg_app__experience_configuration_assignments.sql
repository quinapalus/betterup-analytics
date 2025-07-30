{{
  config(
    tags=["eu"]
  )
}}

WITH experience_configuration_assignments AS (

  select * from {{ source('app', 'experience_configuration_assignments') }}

),

current_experience_configuration_assignments as (

  select
    id AS experience_configuration_assignment_id,
    configurable_id,
    configurable_type,
    template,
    track_id,
    created_at,
    updated_at
  from experience_configuration_assignments

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_experience_configuration_assignments as (
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
    id AS experience_configuration_assignment_id,
    configurable_id,
    configurable_type,
    template,
    track_id,
    created_at,
    updated_at
  from {{ ref('base_app__experience_configuration_assignments_historical') }}
)


select * from archived_experience_configuration_assignments
union
{% endif -%}
select * from current_experience_configuration_assignments

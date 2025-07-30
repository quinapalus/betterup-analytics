WITH reflection_points AS (

  select * from {{ source('app', 'reflection_points') }}

),

current_reflection_points as (

  select
      id AS reflection_point_id,
      coach_assignment_id,
      track_assignment_id,
      {{ load_timestamp('met_prerequisites_at') }},
      {{ load_timestamp('upcoming_at') }},
      {{ load_timestamp('eligible_at') }},
      {{ load_timestamp('coach_due_at') }},
      {{ load_timestamp('canceled_at') }},
      coach_assessment_id,
      user_assessment_id AS member_assessment_id,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from reflection_points

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_reflection_points as (
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
      id AS reflection_point_id,
      coach_assignment_id,
      track_assignment_id,
      {{ load_timestamp('met_prerequisites_at') }},
      {{ load_timestamp('upcoming_at') }},
      {{ load_timestamp('eligible_at') }},
      {{ load_timestamp('coach_due_at') }},
      {{ load_timestamp('canceled_at') }},
      coach_assessment_id,
      user_assessment_id AS member_assessment_id,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__reflection_points_historical') }}
)


select * from archived_reflection_points
union
{% endif -%}
select * from current_reflection_points

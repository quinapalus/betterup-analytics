{{
  config(
    tags=["eu"]
  )
}}


WITH assessments AS (

  select * from {{ source('app', 'assessments') }}

),

current_assessments as (

  select
      ID AS assessment_id,
      user_id,
      creator_id,
      track_assignment_id,
      associated_record_id,
      associated_record_type,
      assessment_configuration_id,
      assessment_configuration_uuid,
      type,
      PARSE_JSON(responses) AS responses,
      parent_id,
      questions_version,
      shared_with_coach,
      {{ load_timestamp('submitted_at') }},
      {{ load_timestamp('expires_at') }},
      {{ load_timestamp('report_generated_at') }},
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from assessments

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_assessments as (
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
      ID AS assessment_id,
      user_id,
      creator_id,
      track_assignment_id,
      associated_record_id,
      associated_record_type,
      assessment_configuration_id,
      assessment_configuration_uuid,
      type,
      PARSE_JSON(responses) AS responses,
      parent_id,
      questions_version,
      shared_with_coach,
      {{ load_timestamp('submitted_at') }},
      {{ load_timestamp('expires_at') }},
      {{ load_timestamp('report_generated_at') }},
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__assessments_historical') }}
)


select * from archived_assessments
union
{% endif -%}
select * from current_assessments


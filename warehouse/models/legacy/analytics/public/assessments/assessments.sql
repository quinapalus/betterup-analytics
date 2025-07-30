--- Do not use -- will be deleted in PR after https://github.com/betterup/betterup-analytics/pull/1114
{{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH assessments AS (
  SELECT * FROM {{ref('int_app__assessments')}}
),

gsheets_assessments AS (
   SELECT * FROM {{ref('stg_gsheets_assessments__assessments')}}
)

SELECT
    a.assessment_id,
    a.creator_id,
    a.type,
    a.questions_version,
    a.created_at,
    a.submitted_at,
    a.expires_at,
    a.updated_at,
    a.parent_id,
    a.report_generated_at,
    a.assessment_configuration_id,
    a.user_id,
    a.associated_record_type,
    a.associated_record_id,
    a_dims.assessment_name,
    a_dims.user_role,
    a.responses,
    a.track_assignment_id
FROM assessments AS a
LEFT OUTER JOIN gsheets_assessments AS a_dims
    ON a.type = a_dims.assessment_type
WHERE a.submitted_at IS NOT NULL

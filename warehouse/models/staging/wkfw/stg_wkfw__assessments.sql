WITH assessments AS (

  SELECT * FROM {{ source('wkfw', 'assessments') }}

)


SELECT
  id AS assessment_id,
  assessment_configuration_id,
  user_id,
  responses,
  {{ load_timestamp('submitted_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM assessments

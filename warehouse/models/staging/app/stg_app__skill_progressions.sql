{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH skill_progressions AS (

  SELECT * FROM {{ source('app', 'skill_progressions') }}

)


SELECT
  id AS skill_progression_id,
  user_id AS member_id,
  skill_id,
  total_minutes,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM skill_progressions

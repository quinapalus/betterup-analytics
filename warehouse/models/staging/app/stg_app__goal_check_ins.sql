{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH goal_check_ins AS (

  SELECT * FROM {{ source('app', 'goal_check_ins') }}

)


SELECT
  id AS goal_check_in_id,
  objective_id AS goal_id,
  user_id AS creator_id,
  progress_response AS progress_response_percent,
  objective_check_in_emotion_id AS goal_check_in_emotion_id,
  {{ load_timestamp('expired_at') }},
  {{ load_timestamp('completed_at') }},
  feedback,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM goal_check_ins

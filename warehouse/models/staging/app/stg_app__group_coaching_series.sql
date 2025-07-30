{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH group_coaching_series AS (

  SELECT * FROM {{ source('app', 'group_coaching_series') }}

)

SELECT
  id AS group_coaching_series_id,
  group_coaching_curriculum_id,
  {{ load_timestamp('registration_start') }},
  {{ load_timestamp('registration_end') }},
  {{ load_timestamp('registration_ended_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM group_coaching_series
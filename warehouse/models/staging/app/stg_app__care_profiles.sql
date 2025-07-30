{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH care_profiles AS (

  SELECT * FROM {{ source('app', 'care_profiles') }}

)


SELECT
  id AS care_profile_id,
  wellness_score_onboarding,
  mood_count,
  current_mood_score,
  current_wellness_score,
  {{ load_timestamp('completed_care_onboarding_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('skipped_care_onboarding_at') }},
  {{ load_timestamp('updated_at') }},
  {{ load_timestamp('mood_last_updated_at') }},
  {{ load_timestamp('current_wellness_score_at') }}
FROM care_profiles
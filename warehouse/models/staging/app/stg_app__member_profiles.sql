{{
  config(
    tags=['classification.c1_highly_restricted']
  )
}}

WITH src_member_profiles AS (

  SELECT * FROM {{ source('app', 'member_profiles') }}

)


SELECT
  id AS member_profile_id,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }},
  people_manager,
  manages_other_managers,
  {{ load_timestamp('next_member_nps_at') }},
  career_topic_theme_id,
  career_development_topic_id,
  {{ load_timestamp('career_topic_theme_picked_at') }},
  career_activity_id,
  coach_race_ethnicity_preferences,
  coach_gender_preferences,
  show_product_market_fit_prompt,
  self_reported_organization_name
FROM src_member_profiles

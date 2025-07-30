{{
  config(
    tags=["eu"]
  )
}}

WITH dedicated_configurations AS (

  SELECT * FROM {{ source('app', 'dedicated_configurations') }}

)


SELECT
  id AS dedicated_configuration_id,
  member_levels,
  languages,
  staffing_risk_levels,
  staffing_tiers,
  staffing_industries,
  num_reflection_points,
  reflection_point_interval_days,
  reflection_point_interval_appointments,
  one_month_survey_enabled,
  cached_tag_list,
  cached_account_tag_list,
  cached_certification_tag_list,
  cached_focus_tag_list,
  cached_postgrad_tag_list,
  cached_product_tag_list,
  cached_professional_tag_list,
  cached_segment_tag_list,
  client_limit
FROM dedicated_configurations
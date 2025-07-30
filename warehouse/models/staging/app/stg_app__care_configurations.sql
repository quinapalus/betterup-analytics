{{
  config(
    tags=["eu"]
  )
}}

WITH care_configurations AS (

  SELECT * FROM {{ source('app', 'care_configurations') }}

)


SELECT
  id AS care_configuration_id,
  member_levels,
  languages,
  staffing_risk_levels,
  staffing_tiers,
  staffing_industries,
  cached_tag_list,
  cached_account_tag_list,
  cached_certification_tag_list,
  cached_focus_tag_list,
  cached_postgrad_tag_list,
  cached_product_tag_list,
  cached_professional_tag_list,
  cached_segment_tag_list,
  client_limit
FROM care_configurations
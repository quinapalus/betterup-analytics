{{
  config(
    tags=["eu"]
  )
}}

WITH on_demand_configurations AS (

  SELECT * FROM {{ source('app', 'on_demand_configurations') }}

)


SELECT
  id AS on_demand_configuration_id,
  member_levels,
  languages,
  staffing_risk_levels,
  staffing_tiers,
  staffing_industries,
  cached_on_demand_tag_list,
  client_limit
FROM on_demand_configurations
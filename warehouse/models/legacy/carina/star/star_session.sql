SELECT
{{ dbt_utils.star(from=ref('fact_session'), relation_alias='fs') }},
--BRING IN ONLY DIM_MEMBER COLUMNS FROM THE REFACTOR (to be deprecated)
  dm.app_member_id,
  dm.member_geo,
  dm.member_subregion_m49,
  dm.employee_level,
  --16 HCM attributes
  dm.member_hcm_attribute_work_location,
  dm.member_hcm_attribute_city,
  dm.member_hcm_attribute_country,
  dm.member_hcm_attribute_region,
  dm.member_hcm_attribute_job_family,
  dm.member_hcm_attribute_job_code,
  dm.member_hcm_attribute_cost_center,
  dm.member_hcm_attribute_business_unit,
  dm.member_hcm_attribute_group,
  dm.member_hcm_attribute_market,
  dm.member_hcm_attribute_employee_type,
  dm.member_hcm_attribute_department,
  dm.member_hcm_attribute_level,
  dm.member_hcm_attribute_is_manager,
  dm.member_hcm_attribute_brand,
  dm.member_hcm_attribute_gender,
{{ dbt_utils.star(from=ref('dim_coach'), except=["COACH_KEY"], relation_alias='dc') }},
{{ dbt_utils.star(from=ref('dim_development_topic'), except=["DEVELOPMENT_TOPIC_KEY"], relation_alias='dt') }},
{{ dbt_utils.star(from=ref('dim_date'), except=["DATE_KEY"], relation_alias='dd') }},
{{ dbt_utils.star(from=ref('dim_account'), except=["ACCOUNT_KEY"], relation_alias='da') }},
{{ dbt_utils.star(from=ref('dim_deployment'), except=["DEPLOYMENT_KEY"], relation_alias='ddm') }},
{{ dbt_utils.star(from=ref('dim_member_deployment'), except=["MEMBER_DEPLOYMENT_KEY", ], relation_alias='dmd') }}
FROM {{ref('fact_session')}} AS fs
INNER JOIN {{ref('dim_members')}} AS dm
  ON fs.member_key = dm.member_key
INNER JOIN {{ref('dim_coach')}} AS dc
  ON fs.coach_key = dc.coach_key
INNER JOIN {{ref('dim_date')}} AS dd
  ON fs.date_key = dd.date_key
INNER JOIN {{ref('dim_account')}} AS da
  ON fs.account_key = da.account_key
INNER JOIN {{ref('dim_deployment')}} AS ddm
  ON fs.deployment_key = ddm.deployment_key
INNER JOIN {{ref('dim_member_deployment')}} AS dmd
  ON fs.member_deployment_key = dmd.member_deployment_key
INNER JOIN {{ref('dim_development_topic')}} AS dt
  ON fs.development_topic_key = dt.development_topic_key

{{
  config(
    tags=['classification.c3_confidential']
  )
}}


SELECT
{{ dbt_utils.star(from=ref('fact_member_satisfaction_response'), relation_alias='fmr') }},
{{ dbt_utils.star(from=ref('dim_coach'), except=["COACH_KEY"], relation_alias='dc') }},
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
{{ dbt_utils.star(from=ref('dim_date'), except=["DATE_KEY"], relation_alias='dd') }},
{{ dbt_utils.star(from=ref('dim_account'), except=["ACCOUNT_KEY"], relation_alias='da') }},
{{ dbt_utils.star(from=ref('dim_deployment'), except=["DEPLOYMENT_KEY"], relation_alias='DP') }},
{{ dbt_utils.star(from=ref('dim_member_deployment'), except=["MEMBER_DEPLOYMENT_KEY"], relation_alias='dmd') }},
{{ dbt_utils.star(from=ref('dim_assessment_item'), except=["ASSESSMENT_ITEM_KEY", "ASSESSMENT_ITEM_CATEGORY"], relation_alias='di') }}
FROM {{ref('fact_member_satisfaction_response')}} AS fmr
INNER JOIN {{ref('dim_coach')}} AS dc
  ON fmr.coach_key = dc.coach_key
INNER JOIN {{ref('dim_members')}} AS dm
  ON fmr.member_key = dm.member_key
INNER JOIN {{ref('dim_date')}} AS dd
  ON fmr.date_key = dd.date_key
INNER JOIN {{ref('dim_account')}} AS da
  ON fmr.account_key = da.account_key
INNER JOIN {{ref('dim_deployment')}} AS dp
  ON fmr.deployment_key = dp.deployment_key
INNER JOIN {{ref('dim_member_deployment')}} AS dmd
  ON fmr.member_deployment_key = dmd.member_deployment_key
INNER JOIN {{ref('dim_assessment_item')}} AS di
  ON fmr.assessment_item_key = di.assessment_item_key
  AND fmr.assessment_item_category = di.assessment_item_category

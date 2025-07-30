{{
  config(
    tags=['classification.c3_confidential'],
    materialized='incremental',
    unique_key='coach_key'
  )
}}

WITH dbt_coach AS (

  SELECT * FROM {{ref('dbt_coach')}}

),

dim_date AS (

  SELECT * FROM {{ref('dim_date')}}

)

-- load priority languages into jinja variable that we can iterate through in SELECT statement
{% set priority_languages = dbt_utils.get_column_values(table=ref('bu_priority_staffing_languages'), column='language') %}


SELECT
  c.coach_key,
  c.email AS coach_email,
  c.first_name AS coach_first_name,
  c.last_name AS coach_last_name,
  c.first_name || ' ' || c.last_name AS coach_name,
  c.is_in_network AS coach_is_currently_in_network,
  c.type_primary AS coach_is_type_primary,
  c.type_extended_network AS coach_is_type_extended_network,
  c.application_date_key,
  COALESCE(ad.fiscal_year_quarter, 'Unknown') AS application_fiscal_year_quarter,
  c.hire_date_key,
  COALESCE(hd.fiscal_year_quarter, 'N/A') AS hire_fiscal_year_quarter,
  c.coach_geo,
  c.coach_subregion_m49,
  c.coach_country_code,
  c.coach_country_name,
  c.coach_geo_country_code,
  array_to_string(c.staffing_languages, ',') AS coach_staffing_languages,
  -- create a boolean field for each priority language using jinja variable defined above
  {% for language in priority_languages %}
  -- use IS TRUE to map NULL input values for Pipeline coaches to false
    coalesce(
        array_contains('{{ language }}'::variant, c.staffing_languages) = true, false)
            AS coach_is_staffable_language_{{ language }},
  {% endfor %}
  -- create boolean fields for selected certifications
  {% for certification in ['mbti', 'icf_acc', 'icf_mcc', 'icf_pcc'] %}
    coalesce(
        (array_contains('certification_{{ certification }}'::variant,
             c.staffing_qualifications) = true),false) AS coach_is_certified_{{ certification }},
  {% endfor %}
  c.bio AS coach_bio,
  c.fnt_applicant_id AS coach_fnt_applicant_id,
  c.app_coach_id AS coach_app_coach_id
FROM dbt_coach AS c
LEFT OUTER JOIN dim_date AS ad
  ON c.application_date_key = ad.date_key
LEFT OUTER JOIN dim_date AS hd
    ON c.hire_date_key = hd.date_key

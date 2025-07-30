{{
  config(
    materialized='view'
  )
}}

WITH coaches AS (
  SELECT * FROM {{ref('dei_coaches')}}
),
{% if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
coach_applicants AS (
  SELECT * FROM {{ref('dei_coach_applicants')}}
),
{% endif %}
coach_capacity_patch AS (

  SELECT
    -- patch Global Staffing Extended Nework hack
    coach_id AS _coach_id,
    CASE
      WHEN max_member_count IN (200, 500) THEN 30
      ELSE max_member_count
    END AS seats_desired_count,
    seats_occupied_count,
    CASE
      WHEN max_member_count IN (200, 500) THEN GREATEST(30 - seats_occupied_count, 0)
      ELSE seats_available_count
    END AS seats_available_count
  FROM coaches

)

SELECT
  {% if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
    {{ coach_key('ca.fountain_applicant_id', 'c.coach_id') }} AS coach_key,
    -- Attribute Fields --
    COALESCE(c.email, ca.email) AS email,
    COALESCE(c.first_name, ca.first_name) AS first_name,
    COALESCE(c.last_name, ca.last_name) AS last_name,
  {% else %}
    {{ dbt_utils.surrogate_key(['c.coach_id']) }} AS coach_key,
    -- Attribute Fields --
    c.email,
    c.first_name,
    c.last_name,
  {% endif %}
  c.bio,
  -- Status Fields --
  -- is_in_network:
  CASE
    -- extra logic to handle discrepencies between staffable_state and deactivated status
    -- set all deactivated coaches to *not* in network
    WHEN c.deactivated_at IS NOT NULL THEN false
    -- if platform coach, use staffable_state to determine if in network
    WHEN c.staffable_state IS NOT NULL AND c.staffable_state <> 'onboarding'
      THEN c.staffable_state IN ('staffable', 'hold_voluntary', 'hold_involuntary')
    -- if fountain information is available, use applicant stage to determine is in network
    {% if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
    WHEN ca.fountain_applicant_id IS NOT NULL
      THEN ca.stage_name IN ('s2-contract', 's3-train', 's4-finalize') AND
           ca.last_transitioned_at > dateadd('day', -90, current_timestamp)
    {% endif %}
    -- catch all for coach accounts that can't be rationalized
    -- that is, their platform account is in onboarding, but we don't have fountain information
    ELSE false
  END AS is_in_network,
  -- coach_state:
  CASE
    WHEN c.staffable_state IN ('staffable', 'hold_voluntary', 'hold_involuntary') THEN 'Live'
    {% if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
      WHEN ca.stage_name IN ('s2-contract', 's3-train', 's4-finalize') THEN 'Pipeline'
    {% endif %}
    ELSE 'Other' -- could separate this logic from is_in_network
  END AS coach_state,
  -- pipeline_stage:
  CASE
    WHEN c.staffable_state IN ('staffable', 'hold_voluntary', 'hold_involuntary') THEN 'hired'
    {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %}
      ELSE 'N/A'
    {% else %}
      ELSE COALESCE(ca.stage_name, 'N/A')
    {% endif %}
  END AS pipeline_stage,
  -- staffable_state:
  COALESCE(c.staffable_state, 'N/A') AS staffable_state,
  c.staffing_tier,
  -- application_date_key:
  -- only populate if application date is prior to hire date (stub for legacy coaches)
  {% if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
    CASE
      WHEN c.created_at IS NULL OR ca.s1_review_started_at < c.created_at
        THEN {{ date_key('ca.s1_review_started_at') }}
    END AS application_date_key,
    CASE
      WHEN c.created_at IS NULL OR ca.s1_review_started_at < c.created_at
        THEN {{ get_date_difference('ca.s1_review_started_at', 'COALESCE(c.created_at, current_timestamp)') }}
    END AS days_since_application,
  {% else %}
    NULL AS application_date_key,
    NULL AS days_since_application,
  {% endif %}
  -- hire_date_key:
  {{ date_key('c.created_at') }} AS hire_date_key,
  {{ get_date_difference('c.created_at', 'current_timestamp') }} AS days_since_hire,
  -- Coach Geo Fields --
  {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
    COALESCE(c.geo, ca.geo, 'Unknown') AS coach_geo,
    COALESCE(c.subregion_m49, ca.subregion_m49, 'Unknown') AS coach_subregion_m49,
    COALESCE(c.country_code, ca.country_code, 'Unknown') AS coach_country_code,
    COALESCE(c.country_name, ca.country_name, 'Unknown') AS coach_country_name,
    CASE
      WHEN COALESCE(c.geo, ca.geo) IS NOT NULL AND
        COALESCE(c.country_code, ca.country_code) IS NOT NULL
      THEN CONCAT(COALESCE(c.geo, ca.geo), '-', COALESCE(c.country_code, ca.country_code))
      ELSE 'Unknown'
    END AS coach_geo_country_code,
  {% else %}
    COALESCE(c.geo, 'Unknown') AS coach_geo,
    COALESCE(c.subregion_m49, 'Unknown') AS coach_subregion_m49,
    COALESCE(c.country_code, 'Unknown') AS coach_country_code,
    COALESCE(c.country_name, 'Unknown') AS coach_country_name,
    CASE
      WHEN c.geo IS NOT NULL AND
        c.country_code IS NOT NULL
      THEN CONCAT(c.geo, '-', c.country_code)
      ELSE 'Unknown'
    END AS coach_geo_country_code,
  {% endif -%}
  -- Language & Staffing Fields --
  CASE
    WHEN c.staffing_languages IS NOT NULL THEN c.staffing_languages
    {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}  
    ELSE ca.staffing_languages
    {%- endif %}
  END AS staffing_languages,
  {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %}
  c.priority_language,
  {% else %}
  COALESCE(c.priority_language, ca.priority_language) AS priority_language,
  {%- endif %}
  c.staffing_qualifications,
  c.type_primary,
  c.type_extended_network,
  c.type_on_demand,
  COALESCE(c.specialist_verticals, TO_ARRAY('N/A')) AS extended_network_specialist_verticals,
  -- Seat Capacity Fields --
  ccp.seats_desired_count,
  ccp.seats_occupied_count,
  ccp.seats_available_count,
  -- Source System Foreign Keys --
  {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %}
  NULL AS fnt_applicant_id,
  {% else %}
  ca.fountain_applicant_id AS fnt_applicant_id,
  {%- endif %}
  c.coach_id AS app_coach_id,
  c.first_staffable_at,
  c.last_staffable_at
FROM coaches AS c
LEFT OUTER JOIN coach_capacity_patch AS ccp
  ON c.coach_id = ccp._coach_id AND
     -- only bring in capacity for Live coaches
     c.staffable_state IN ('staffable', 'hold_voluntary', 'hold_involuntary')
{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
FULL OUTER JOIN coach_applicants AS ca
  ON c.coach_id = ca.coach_id
{% endif %}

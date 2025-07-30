{{
  config(
    tags=["eu"]
  )
}}

-- To be refactored
-- As we move away from track model usage, this model will be refactored so that the configuration set of tables
-- need not be joined to tracks

WITH tracks AS (

  SELECT * FROM {{ ref('stg_app__tracks') }}

),

member_configurations AS (

  SELECT * FROM {{ ref('int_app__member_configurations') }}

),

coach_configurations AS (

  SELECT * FROM {{ ref('int_app__coach_configurations') }}

),

partner_configurations AS (

  SELECT * FROM {{ ref('int_app__partner_configurations') }}

),

dedicated_configurations AS (

  SELECT * FROM {{ ref('int_app__dedicated_configurations') }}

),

on_demand_configurations AS (

  SELECT * FROM {{ ref('int_app__on_demand_configurations') }}

),

care_configurations AS (

  SELECT * FROM {{ ref('int_app__care_configurations') }}

),

connect_configurations AS (

  SELECT * FROM {{ ref('int_app__connect_configurations') }}

),

admin_configurations AS (

  SELECT * FROM {{ ref('int_app__admin_configurations') }}

),

direct_payer_configurations AS (

  SELECT * FROM {{ ref('int_app__direct_payer_configurations') }}

)


SELECT
  t.track_id,
  cc.overview AS overview,
  mc.default_resource_list_id AS default_resource_list_id,
  mc.includes_behavioral_assessments AS includes_behavioral_assessments,
  dc.reflection_point_interval_days AS reflection_point_interval_days,
  mc.manager_feedback_enabled AS manager_feedback_enabled,
  dc.reflection_point_interval_appointments AS reflection_point_interval_appointments,
  dc.num_reflection_points AS num_reflection_points,
  ac.deployment_type AS deployment_type,
  ac.launches_on AS launches_on_from_config,
  ac.member_orientation AS member_orientation,
  ac.customer_goals AS customer_goals,
  ac.key_satisfaction_driver AS key_satisfaction_driver,
  ac.wpm_behavior_goals AS wpm_behavior_goals,
  ac.deployment_cadence AS deployment_cadence,
  coalesce(mc.whole_person360_enabled, false) AS whole_person360_enabled, -- sometimes NULL in app db, map NULL to FALSE here
  coalesce(mc.whole_person180_enabled, false) AS whole_person180_enabled, -- sometimes NULL in app db, map NULL to FALSE here
  true AS whole_person_model_2,
  ARRAY_CAT(coalesce(crc.member_levels,[]),ARRAY_CAT(coalesce(dc.member_levels,[]),coalesce(odc.member_levels,[]))) AS staffing_member_levels,
  cc.program_briefing_duration_minutes AS program_briefing_duration_minutes,
  cc.program_briefing_automatic_payment AS program_briefing_automatic_payment,
  ARRAY_CAT(coalesce(crc.staffing_risk_levels,[]),ARRAY_CAT(coalesce(dc.staffing_risk_levels,[]),coalesce(odc.staffing_risk_levels,[]))) AS staffing_risk_levels,
  ARRAY_CAT(coalesce(crc.staffing_tiers,[]),ARRAY_CAT(coalesce(dc.staffing_tiers,[]),coalesce(odc.staffing_tiers,[]))) AS staffing_tiers,
  ARRAY_CAT(coalesce(crc.staffing_industries,[]),ARRAY_CAT(coalesce(dc.staffing_industries,[]),coalesce(odc.staffing_industries,[]))) AS staffing_industries,
  dc.client_limit AS client_limit,
  dpc.stripe_pricing_plan_id AS stripe_pricing_plan_id,
  ARRAY_CAT(coalesce(crc.languages,[]),ARRAY_CAT(coalesce(dc.languages,[]),coalesce(odc.languages,[]))) AS staffing_languages,
  concat_ws(',',crc.cached_tag_list,dc.cached_tag_list) AS cached_tag_list,
  concat_ws(',',crc.cached_account_tag_list,dc.cached_account_tag_list) AS cached_account_tag_list,
  concat_ws(',',crc.cached_certification_tag_list,dc.cached_certification_tag_list) AS cached_certification_tag_list,
  concat_ws(',',crc.cached_focus_tag_list,dc.cached_focus_tag_list) AS cached_focus_tag_list,
  concat_ws(',',crc.cached_postgrad_tag_list,dc.cached_postgrad_tag_list) AS cached_postgrad_tag_list,
  concat_ws(',',crc.cached_product_tag_list,dc.cached_product_tag_list) AS cached_product_tag_list,
  concat_ws(',',crc.cached_professional_tag_list,dc.cached_professional_tag_list) AS cached_professional_tag_list,
  concat_ws(',',crc.cached_segment_tag_list,dc.cached_segment_tag_list) AS cached_segment_tag_list,
  ac.internal_notes AS internal_notes,
  pc.downloadable AS downloadable
FROM tracks AS t
LEFT OUTER JOIN member_configurations AS mc
ON mc.track_id = t.track_id
LEFT OUTER JOIN coach_configurations AS cc
ON cc.track_id = t.track_id
LEFT OUTER JOIN partner_configurations AS pc
ON pc.track_id = t.track_id
LEFT OUTER JOIN dedicated_configurations AS dc
ON dc.track_id = t.track_id
LEFT OUTER JOIN on_demand_configurations AS odc
ON odc.track_id = t.track_id
LEFT OUTER JOIN care_configurations AS crc
ON crc.track_id = t.track_id
LEFT OUTER JOIN connect_configurations AS coc
ON coc.track_id = t.track_id
LEFT OUTER JOIN admin_configurations AS ac
ON ac.track_id = t.track_id
LEFT OUTER JOIN direct_payer_configurations AS dpc
ON dpc.track_id = t.track_id
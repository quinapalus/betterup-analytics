{{
  config(
    tags=['classification.c3_confidential','eu']
  )
}}

WITH whole_person_assessment AS (

  SELECT * FROM {{ref('fact_whole_person_assessment')}}

),

whole_person_subdimension_scores AS (

  SELECT * FROM {{ref('dei_whole_person_subdimension_scores')}}

),

dim_date AS (

  SELECT * FROM {{ref('dim_date')}}

),

whole_person_assessment_scores AS (

  -- pre-join assessment meta-data to subdimension scores to simplify repeated
  -- join logic in final SELECT

  SELECT
    wpa.member_key,
    wpa.date_key,
    dd.date,
    wpa.account_key,
    wpa.deployment_key,
    wpa.member_deployment_key,
    ss.whole_person_subdimension_key,
    wpa.assessment_name,
    ss.scale_score,
    wpa.account_assessment_sequence,
    wpa.deployment_assessment_sequence,
    wpa.account_wpm_sequence,
    wpa.member_wpm_sequence,
    wpa.deployment_wpm_sequence,
    wpa.deployment_assessment_reverse_sequence,
    wpa.app_assessment_id
  FROM whole_person_assessment AS wpa
  INNER JOIN whole_person_subdimension_scores AS ss
    ON wpa.app_assessment_id = ss.assessment_id
  INNER JOIN dim_date AS dd
    ON wpa.date_key = dd.date_key

)


SELECT
  wpa.member_key,
  wpa.date_key,
  wpa.account_key,
  wpa.deployment_key,
  wpa.member_deployment_key,
  wpa.whole_person_subdimension_key,
  wpa.assessment_name,
  wpa.scale_score,
  COALESCE(baseline.assessment_name, 'N/A') AS baseline_assessment_name,
  baseline.scale_score AS baseline_scale_score,
  wpa.scale_score - baseline.scale_score AS scale_score_growth_from_baseline,
  {{ get_day_difference('baseline.date', 'wpa.date') }} AS baseline_days_before_current,
  COALESCE(preceding.assessment_name, 'N/A') AS preceding_assessment_name,
  preceding.scale_score AS preceding_scale_score,
  {{ get_day_difference('preceding.date', 'wpa.date') }} AS preceding_days_before_current,
  wpa.scale_score - preceding.scale_score AS scale_score_growth_from_preceding,
  wpa.account_assessment_sequence,
  wpa.deployment_assessment_sequence,
  wpa.account_wpm_sequence,
  wpa.member_wpm_sequence,
  wpa.deployment_wpm_sequence,
  wpa.deployment_assessment_reverse_sequence,
  wpa.app_assessment_id,
  baseline.app_assessment_id AS baseline_app_assessment_id,
  preceding.app_assessment_id AS preceding_app_assessment_id
FROM whole_person_assessment_scores AS wpa
-- join in single baseline subdimension score, matching on member
LEFT OUTER JOIN whole_person_assessment_scores AS baseline
  ON baseline.member_key = wpa.member_key AND
     baseline.whole_person_subdimension_key = wpa.whole_person_subdimension_key AND
     -- select first assessment with unique assessment_id that wasn't submitted on
     -- the same day
     baseline.member_wpm_sequence = 1 AND
     baseline.app_assessment_id != wpa.app_assessment_id AND
     baseline.date_key != wpa.date_key
-- join in single preceding subdimension score, matching on member, WPM version,
-- and subdimension.
LEFT OUTER JOIN whole_person_assessment_scores AS preceding
  ON preceding.member_key = wpa.member_key AND
     preceding.whole_person_subdimension_key = wpa.whole_person_subdimension_key AND
     -- preceding assessment is defined by decrementing member_wpm_sequence by 1
     -- and filtering out any assessments that were submitted on the same day.
     preceding.member_wpm_sequence = wpa.member_wpm_sequence - 1 AND
     preceding.date_key != wpa.date_key

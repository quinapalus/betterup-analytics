WITH coach_nps_assessment_response AS (

  SELECT * FROM {{ref('dbt_coach_nps_assessment_response')}}

)


SELECT
  cr.date_key,
  cr.coach_app_coach_id,
  cr.coach_raw_promoter_score,
  cr.coach_net_promoter_score,
  cr.coach_qualitative_feedback,
  cr.coach_net_promoter_score_category,
  cr.assessment_sequence,
  cr.is_most_recent_assessment,
  preceding.coach_raw_promoter_score AS preceding_assessment_coach_raw_promoter_score,
  preceding.coach_net_promoter_score AS preceding_assessment_coach_net_promoter_score,
  preceding.coach_qualitative_feedback AS preceding_assessment_coach_qualitative_feedback,
  preceding.coach_net_promoter_score_category AS preceding_assessment_coach_net_promoter_score_category,
  preceding.assessment_sequence AS preceding_assessment_sequence,
  {{ get_day_difference('preceding.app_assessment_submitted_at', 'cr.app_assessment_submitted_at') }} AS preceding_days_before_current,
  cr.coach_raw_promoter_score - preceding.coach_raw_promoter_score AS raw_promoter_score_growth_from_preceding,
  cr.coach_net_promoter_score_category <> preceding.coach_net_promoter_score_category AS has_coach_net_promoter_score_category_changed,
  cr.app_assessment_id,
  preceding.app_assessment_id AS preceding_app_assessment_id,
  preceding.app_assessment_submitted_at AS preceding_app_assessment_submitted_at
FROM coach_nps_assessment_response AS cr
LEFT OUTER JOIN coach_nps_assessment_response AS preceding
  ON cr.coach_app_coach_id = preceding.coach_app_coach_id
  -- using assessment sequence, we join on the assessment that has assessment sequence - 1
  -- as that will get us to the preceding assessment.
  AND preceding.assessment_sequence = cr.assessment_sequence - 1

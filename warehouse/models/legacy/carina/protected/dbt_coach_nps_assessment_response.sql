WITH coach_satisfaction_response AS (

  SELECT * FROM {{ref('fact_coach_satisfaction_response')}}

),

coach_qualitative_item_response AS (

  SELECT * FROM {{ref('fact_coach_qualitative_item_response')}}

)


SELECT
  sr.date_key,
  sr.coach_app_coach_id,
  sr.assessment_item_response AS coach_raw_promoter_score,
  sr.assessment_item_score AS coach_net_promoter_score,
  qr.assessment_item_response AS coach_qualitative_feedback,
  sr.coach_response_category AS coach_net_promoter_score_category,
  ROW_NUMBER() OVER (PARTITION BY sr.coach_app_coach_id ORDER BY sr.app_assessment_submitted_at ASC) AS assessment_sequence,
  (ROW_NUMBER() OVER (PARTITION BY sr.coach_app_coach_id ORDER BY sr.app_assessment_submitted_at DESC)) = 1 AS is_most_recent_assessment,
  sr.app_assessment_id,
  sr.app_assessment_submitted_at
FROM coach_satisfaction_response AS sr
LEFT OUTER JOIN coach_qualitative_item_response AS qr
  ON sr.app_assessment_id = qr.app_assessment_id

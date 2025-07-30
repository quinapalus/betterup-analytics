WITH coach_satisfaction_assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}
  -- extract only coach NPS assessments and relevant
  -- item keys
  WHERE type = 'Assessments::CoachNpsAssessment'
    AND item_key = 'platform_nps'

),

dim_assessment_item AS (

  SELECT * FROM {{ref('dim_assessment_item')}}
  -- choose Coach Satisfaction and exclude
  -- text responses in this model.
  WHERE assessment_item_category = 'Coach Satisfaction'
    AND assessment_item_response_scale <> 'text_response'

)


SELECT
  {{ date_key('ci.submitted_at') }} AS date_key,
  ci.user_id AS coach_app_coach_id,
  ai.assessment_item_key,
  ai.assessment_item_category,
  ci.item_response AS assessment_item_response,
  {{ score_nps ('ci.item_response') }} AS assessment_item_score,
  {{ sanitize_nps_response_category('ci.item_response') }} AS coach_response_category,
  ci.assessment_id AS app_assessment_id,
  ci.submitted_at AS app_assessment_submitted_at
FROM coach_satisfaction_assessment_items AS ci
INNER JOIN dim_assessment_item AS ai
  ON ci.item_key = ai.assessment_item_key

WITH coach_qualitative_assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}
  -- extract only coach NPS assessments and relevant
  -- item keys
  WHERE type = 'Assessments::CoachNpsAssessment'
    AND item_key = 'feedback'

),

dim_assessment_item AS (

  SELECT * FROM {{ref('dim_assessment_item')}}
  -- choose Coach Satisfaction and include only
  -- text responses in this model.
  WHERE assessment_item_category = 'Coach Satisfaction'
    AND assessment_item_response_scale = 'text_response'

)


SELECT
  {{ dbt_utils.surrogate_key(['ci.submitted_at', 'ci.user_id', 'ci.assessment_id', 'ai.assessment_item_key']) }} as primary_key,
  {{ date_key('ci.submitted_at') }} AS date_key,
  ci.user_id AS coach_app_coach_id,
  ai.assessment_item_key,
  ai.assessment_item_category,
  ci.item_response AS assessment_item_response,
  {{ word_count('ci.item_response' )}} AS assessment_item_response_word_count,
  ci.assessment_id AS app_assessment_id,
  ci.submitted_at AS app_assessment_submitted_at
FROM coach_qualitative_assessment_items AS ci
INNER JOIN dim_assessment_item AS ai
  ON ci.item_key = ai.assessment_item_key

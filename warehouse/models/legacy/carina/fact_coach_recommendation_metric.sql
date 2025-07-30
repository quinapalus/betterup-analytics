WITH coach_recommendations AS (

  SELECT * FROM {{ ref('dbt_coach_recommendations')}}

)


SELECT
  -- Surrogate Primary Key of COACH_APP_COACH_ID, MEMBER_KEY, APP_COACH_RECOMMENDATION_ID, COACH_RECOMMENDATION_POSITION, COACH_ASSIGNMENT_DURATION_DAYS, COACH_SELECTED_DATE
  {{ dbt_utils.surrogate_key(['coach_id', 'member_id', 'coach_recommendation_id', 'position', 'coach_assignment_duration_days', 'selected_at']) }} AS fact_coach_recommendation_metric_id,
  coach_id AS coach_app_coach_id,
  {{ date_key('recommended_at')}} AS date_key,
  {{ member_key('member_id') }} AS member_key,
  position AS coach_recommendation_position,
  is_coach_selected,
  is_coach_assignment_ended,
  coach_assignment_duration_days,
  coach_recommendation_id AS app_coach_recommendation_id,
  DATE_TRUNC('DAY', recommended_at) AS coach_recommended_date, -- keep the date since we need to aggregate the model later based on date
  DATE_TRUNC('DAY', selected_at) AS coach_selected_date
FROM coach_recommendations

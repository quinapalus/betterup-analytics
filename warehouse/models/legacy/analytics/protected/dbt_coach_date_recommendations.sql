WITH coach_recommendations AS (

  SELECT * FROM {{ref('dbt_coach_recommendations')}}

)


SELECT
  coach_id,
  DATE_TRUNC('DAY', recommended_at) AS date_day,
  COUNT(coach_recommendation_id) AS recommendation_count
FROM coach_recommendations
GROUP BY coach_id, DATE_TRUNC('DAY', recommended_at)

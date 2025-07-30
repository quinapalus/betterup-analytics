{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH fact_member_satisfaction_response AS (

  SELECT * FROM {{ref('fact_member_satisfaction_response')}}

),

dim_date AS (

  SELECT * FROM {{ref('dim_date')}}

)

SELECT
  fr.coach_key,
  fr.assessment_item_key,
  COUNT(fr.assessment_item_key) AS assessment_item_response_count,
  AVG(fr.assessment_item_score) AS assessment_item_score_mean,
  COUNT(CASE WHEN dd.date > dateadd('day', -90, current_timestamp) THEN fr.assessment_item_key END) AS assessment_item_response_count_t90d,
  AVG(CASE WHEN dd.date > dateadd('day', -90, current_timestamp)  THEN fr.assessment_item_score END) AS assessment_item_score_mean_t90d
FROM fact_member_satisfaction_response AS fr
INNER JOIN dim_date AS dd
  ON fr.date_key = dd.date_key
GROUP BY fr.coach_key, fr.assessment_item_key

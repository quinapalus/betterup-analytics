WITH coach AS (

  SELECT * FROM {{ref('dbt_coach')}}
  -- only select coaches that have been hired
  WHERE app_coach_id IS NOT NULL

),

coach_recommendation_metric AS (

  SELECT * FROM {{ref('fact_coach_recommendation_metric')}}

)


SELECT
  c.app_coach_id AS coach_app_coach_id,
  -- General recommendation metrics
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS recommendation_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS recommendation_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS recommendation_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS recommendation_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS recommendation_count_t90d,
  -- Recommendation t14d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
    THEN app_coach_recommendation_id END), 0) AS recommendation_first_position_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
    THEN app_coach_recommendation_id END), 0) AS recommendation_second_position_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_third_position_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_other_position_count_t14d,
  -- Recommendation t21d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
    THEN app_coach_recommendation_id END), 0) AS recommendation_first_position_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
    THEN app_coach_recommendation_id END), 0) AS recommendation_second_position_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_third_position_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_other_position_count_t21d,
  -- Recommendation t30d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
    THEN app_coach_recommendation_id END), 0) AS recommendation_first_position_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
    THEN app_coach_recommendation_id END), 0) AS recommendation_second_position_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_third_position_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_other_position_count_t30d,
  -- Recommendation t60d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
    THEN app_coach_recommendation_id END), 0) AS recommendation_first_position_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
    THEN app_coach_recommendation_id END), 0) AS recommendation_second_position_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_third_position_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_other_position_count_t60d,
  -- Recommendation t90d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
    THEN app_coach_recommendation_id END), 0) AS recommendation_first_position_count_t90d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
    THEN app_coach_recommendation_id END), 0) AS recommendation_second_position_count_t90d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_third_position_count_t90d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
    THEN app_coach_recommendation_id END), 0) AS recommendation_other_position_count_t90d,
  -- General selection after recommendation metrics
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_count_t90d,
  -- Selection after recommendation t14d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_first_position_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_second_position_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_third_position_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_other_position_count_t14d,
  -- Selection after recommendation t21d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_first_position_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_second_position_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_third_position_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_other_position_count_t21d,
  -- Selection after recommendation t30d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_first_position_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_second_position_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_third_position_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_other_position_count_t30d,
   -- Selection after recommendation t60d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_first_position_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_second_position_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_third_position_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_other_position_count_t60d,
  -- Selection after recommendation t90d metrics with positions
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position = 1
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_first_position_count_t90d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position = 2
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_second_position_count_t90d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position = 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_third_position_count_t90d,
  COALESCE(COUNT(CASE WHEN coach_recommended_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND
    coach_selected_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP) AND coach_recommendation_position > 3
      THEN app_coach_recommendation_id END), 0) AS selection_after_recommendation_other_position_count_t90d,
  -- General selection (independent of recommendation) metrics
  COALESCE(COUNT(CASE WHEN coach_selected_date >= DATEADD('DAY', -14, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS selection_count_t14d,
  COALESCE(COUNT(CASE WHEN coach_selected_date >= DATEADD('DAY', -21, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS selection_count_t21d,
  COALESCE(COUNT(CASE WHEN coach_selected_date >= DATEADD('DAY', -30, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS selection_count_t30d,
  COALESCE(COUNT(CASE WHEN coach_selected_date >= DATEADD('DAY', -60, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS selection_count_t60d,
  COALESCE(COUNT(CASE WHEN coach_selected_date >= DATEADD('DAY', -90, CURRENT_TIMESTAMP)
    THEN app_coach_recommendation_id END), 0) AS selection_count_t90d,
  -- First and most recent timestamps
  MIN(coach_recommended_date) AS first_coach_recommended_date,
  MAX(coach_recommended_date) AS last_coach_recommended_date,
  MIN(coach_selected_date) AS first_coach_selected_date,
  MAX(coach_selected_date) AS last_coach_selected_date
FROM coach AS c
LEFT OUTER JOIN coach_recommendation_metric AS cm
  ON c.app_coach_id = cm.coach_app_coach_id
GROUP BY c.app_coach_id

WITH coach_recommendations AS (

  SELECT * FROM {{ref('stg_app__coach_recommendations')}}
  -- filter out BetterUp support
  WHERE coach_id NOT IN (389, 5427)

),

coach_assignments AS (

  SELECT * FROM {{ref('dim_coach_assignments')}}
  -- note that support is already filtered out in this model

)


SELECT
  cr.coach_recommendation_id,
  cr.coach_id,
  cr.member_id,
  cr.created_at AS recommended_at,
  cr.algorithm,
  cr.position,
  cr.overall_score,
  ca.coach_id IS NOT NULL AS is_coach_selected,
  ca.created_at AS selected_at,
  ca.is_coach_assignment_ended,
  ca.coach_assignment_duration_days
FROM coach_recommendations AS cr
LEFT OUTER JOIN coach_assignments AS ca
  ON cr.coach_recommendation_id = ca.coach_recommendation_id

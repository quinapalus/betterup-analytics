WITH coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}

),

coach_recommendations AS (

  SELECT * FROM {{ref('stg_app__coach_recommendations')}}

),

members AS (

  SELECT * FROM {{ref('dei_members')}}

),

coaches AS (

  SELECT * FROM {{ref('dei_coaches')}}

),

selected_coaches AS (

  SELECT
    ca.coach_id AS selected_coach_id,
    ca.coach_recommendation_id,
    cr.coach_recommendation_set_id,
    ca.created_at
  FROM coach_assignments AS ca
  INNER JOIN coach_recommendations AS cr
    ON ca.coach_recommendation_id = cr.coach_recommendation_id
  WHERE ca.member_id IN (SELECT member_id from members)

),

coach_recommendation_info AS (
  -- Find the total number of times a coach has been recommended and
  -- how many times they've been selected from those sets.
  SELECT
    cr.coach_id,
    MIN(cr.created_at) as first_recommended_at,
    MIN(CASE WHEN cr.coach_id = pc.selected_coach_id then pc.created_at ELSE NULL END) as first_selected_at,
    MAX(cr.created_at) as last_recommended_at,
    COUNT(*) as appearances_count,
    COUNT(CASE WHEN cr.coach_id = pc.selected_coach_id then 1 ELSE NULL END) as selected_count
  FROM coach_recommendations AS cr
  LEFT JOIN selected_coaches AS pc
    ON cr.coach_recommendation_set_id = pc.coach_recommendation_set_id
  GROUP BY cr.coach_id

),

first_assignment AS (

  SELECT
   coach_id,
   MIN(created_at) AS first_assigned_at
  FROM coach_assignments
  WHERE member_id IN (SELECT member_id from members)
  GROUP BY coach_id

),

first_appeared_at AS (

  SELECT
    i.coach_id,
    -- c.first_staffable_at may be null, use
    -- i.first_recommended_at to sanitize those fields (will always be non-NULL)
    LEAST(
      COALESCE(c.first_staffable_at, i.first_recommended_at),
      i.first_recommended_at
      ) AS first_appeared_at
  FROM
    coach_recommendation_info AS i
  LEFT JOIN coaches as c
    ON c.coach_id = i.coach_id

)


SELECT
  i.coach_id,
  i.appearances_count,
  i.selected_count,
  i.selected_count::numeric / i.appearances_count as selected_rate,
  ft.first_appeared_at,
  fa.first_assigned_at,
  i.first_recommended_at,
  i.first_selected_at,
  i.last_recommended_at,
  datediff('day', ft.first_appeared_at, fa.first_assigned_at) AS time_to_first_assignment_days,
  datediff('day', ft.first_appeared_at, i.first_recommended_at) AS time_to_first_recommendation_days
FROM
  coach_recommendation_info i
LEFT JOIN first_appeared_at as ft
  ON ft.coach_id = i.coach_id
LEFT JOIN first_assignment as fa
  ON fa.coach_id = i.coach_id
ORDER BY
  i.appearances_count DESC

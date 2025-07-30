WITH coach_assignments AS (

  SELECT * FROM {{ref('dim_coach_assignments')}}

)


SELECT
  coach_id,
  DATE_TRUNC('DAY', created_at) AS date_day,
  COUNT(coach_assignment_id) AS assignment_count
FROM coach_assignments
GROUP BY coach_id, DATE_TRUNC('DAY', created_at)

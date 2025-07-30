{{
  config(
    tags=["eu"]
  )
}}

WITH coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}
  -- filter out BetterUp support
  WHERE coach_id NOT IN (389, 5427)

)

SELECT
  -- use unique combination of member, coach, and day of assignment creation
  coach_assignment_id,
  member_id,
  coach_id,
  created_at,
  ended_at,
  ended_reason,
  ended_at IS NOT NULL AS is_coach_assignment_ended,
  {{ get_day_difference ('created_at', 'COALESCE(ended_at, CURRENT_TIMESTAMP)', rounding_function = 'CEIL')}} AS coach_assignment_duration_days,
  role,
  case when role = 'group' then TRUE else FALSE end as is_group_coaching_assignment,
  coach_recommendation_id,
  group_coaching_registration_id
FROM coach_assignments
-- order according to row number requirements, but add additional descending-based ordering
-- to prioritize open coach assignment or the assignment with the most recent date.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY member_id, coach_id, DATE_TRUNC('DAY', created_at)
    ORDER BY member_id, coach_id, DATE_TRUNC('DAY', created_at) ASC, COALESCE(ended_at, current_timestamp) DESC
) = 1

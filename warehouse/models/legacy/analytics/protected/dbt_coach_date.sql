WITH coaches AS (

  SELECT * FROM {{ref('dei_coaches')}}

),

days AS (

  SELECT date_day FROM {{ref('util_day')}}

)


SELECT
  c.coach_id,
  d.date_day as day
FROM coaches AS c
LEFT OUTER JOIN days AS d
  ON c.created_at <= d.date_day
  AND d.date_day < COALESCE(c.deactivated_at, current_date)

WITH coach_profiles AS (

  SELECT * FROM {{ref('int_coach__coach_profiles')}}

),

coach_profile_specialist_verticals AS (

  SELECT * FROM {{ref('stg_coach__coach_profile_specialist_verticals')}}

),

specialist_verticals AS (

  SELECT * FROM {{ref('stg_curriculum__specialist_verticals')}}

)


SELECT
  cp.coach_id,
  cv.specialist_vertical_uuid,
  cv.specialist_vertical_id,
  sv.name AS specialist_vertical,
  cv.created_at,
  cv.updated_at
FROM coach_profiles AS cp
INNER JOIN coach_profile_specialist_verticals AS cv
  ON cp.coach_profile_uuid = cv.coach_profile_uuid
INNER JOIN specialist_verticals AS sv
  ON cv.specialist_vertical_uuid = sv.specialist_vertical_uuid

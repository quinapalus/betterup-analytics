WITH track_enrollments AS (

  SELECT * FROM {{ref('dbt_track_enrollments')}}

),

member_track_upcoming_engagement AS (

  SELECT * FROM {{ref('dbt_member_track_upcoming_engagement')}}

),

members AS (

  SELECT * FROM {{ref('dei_members')}}

),

dim_member AS (

  SELECT * FROM {{ref('dim_members')}}

),

dim_deployment AS (

  SELECT * FROM {{ref('dim_deployment')}}

)


SELECT
  {{ member_deployment_key ('te.member_id', 'te.track_id') }}  AS member_deployment_key,
  te.invited_at::DATE AS deployment_invite_date,
  te.ended_at IS NULL AS member_is_open_on_deployment,
  te.ended_at::DATE AS deployment_ended_date,
  m.is_activated AS member_is_activated_on_deployment,
  -- use COALESCE to complement LEFT OUTER JOIN for members with closed enrollments
  -- not captured in the dbt_member_track_upcoming_engagement model.
  COALESCE(mt.has_upcoming_session, false) AS member_has_upcoming_session_on_deployment,
  mt.is_upcoming_session_recurring AS member_has_recurring_upcoming_session_on_deployment,
  {{ sanitize_session_type ('mt.upcoming_session_type') }} AS upcoming_session_on_deployment_type,
  mt.upcoming_session_starts_at::DATE AS upcoming_session_on_deployment_date,
  te.is_hidden AS member_is_hidden_on_deployment
FROM track_enrollments AS te
INNER JOIN members AS m
  ON te.member_id = m.member_id
LEFT OUTER JOIN member_track_upcoming_engagement AS mt
  ON te.member_id = mt.member_id
  AND te.track_id = mt.track_id
-- apply INNER JOINs to dim member and dim deployment
-- to ensure the data in those models is strictly a subset
-- of the data in this model.
INNER JOIN dim_member AS dm
  ON {{ member_key('te.member_id') }} = dm.member_key
INNER JOIN dim_deployment AS dd
  ON {{ deployment_key('te.track_id') }} = dd.deployment_key

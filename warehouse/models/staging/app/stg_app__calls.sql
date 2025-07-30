WITH calls AS (

  SELECT * FROM {{ source('app', 'calls') }}

)


SELECT
  id AS call_id,
  user_id AS member_id,
  coach_id,
  {{ load_timestamp('user_connected_at', 'member_connected_at') }},
  {{ load_timestamp('user_last_disconnect_at', 'member_last_disconnect_at') }},
  PARSE_JSON(user_client_info) AS member_client_info,
  {{ load_timestamp('coach_connected_at') }},
  {{ load_timestamp('coach_last_disconnect_at') }},
  PARSE_JSON(coach_client_info) AS coach_client_info,
  coach_reported_problem,
  completed_on_platform,
  completed_on_phone,
  archive_id,
  PARSE_JSON(issues) AS issues,
  media_mode,
  session_id AS tokbox_session_id,
  platform_provider,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM calls

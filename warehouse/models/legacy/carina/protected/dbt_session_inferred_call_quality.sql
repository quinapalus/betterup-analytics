WITH session AS (

  SELECT * FROM {{ref('dbt_session')}}
  -- select dates during which we had to infer call quality
  -- from coach feedback
  WHERE app_starts_at < '01/18/2020'

),

completed_sessions_platform AS (
  -- define primary CTE with platform information
  SELECT
    session_key,
    member_connected,
    coach_connected,
    coach_feedback_call_quality,
    CASE
      -- off platform: neither connected
      WHEN member_connected = false AND coach_connected = false THEN 'Off Platform'
      WHEN member_connected = true AND coach_connected = true AND
      -- on platform: both connected, reported slightly longer than actual
      (reported_duration_minutes - platform_duration_minutes < 10 OR
      -- on platform: both connected, actual longer than reported
      platform_duration_minutes > reported_duration_minutes) THEN 'On Platform'
      -- attempted platform: all other cases
      ELSE 'Attempted Platform'
    END AS platform
  FROM session

),

completed_sessions_tech_issues AS (
  -- define intermediary CTE with tech issue information
  SELECT
    session_key,
    member_connected,
    coach_connected,
    coach_feedback_call_quality,
    platform,
    CASE
      -- do not report for off-platform calls
      WHEN platform = 'Off Platform' THEN NULL
      -- always consider true for attempted-platform calls
      WHEN platform = 'Attempted Platform' THEN true
      -- use coach feedback for on-platform calls
      WHEN coach_feedback_call_quality IN ('didnt_use', 'no_problems') OR coach_feedback_call_quality IS NULL THEN false
      ELSE true
    END AS has_tech_issues
  FROM completed_sessions_platform

),

completed_sessions_tech_issue_type AS (
  -- define secondary CTE with tech issue information,
  -- as well as information on issue type.
  SELECT
    session_key,
    platform,
    has_tech_issues,
    CASE
      WHEN has_tech_issues = false OR has_tech_issues IS NULL THEN NULL
      -- use coach feedback for on-platform calls
      WHEN has_tech_issues = true AND platform = 'On Platform' THEN coach_feedback_call_quality
      -- if informative, use coach feedback for attempted-platform calls
      WHEN
        (has_tech_issues = true AND
         platform = 'Attempted Platform' AND
         coach_feedback_call_quality IN ('connect_problem', 'connection_quality', 'no_audio', 'no_video', 'poor_audio', 'call_dropped'))
        THEN coach_feedback_call_quality
      -- otherwise, use internal designation for attempted-platform calls
      ELSE (
        CASE
          WHEN member_connected = true AND coach_connected = true THEN 'both_connected_switched'
          WHEN member_connected = false THEN 'member_not_connected'
          ELSE 'coach_not_connected'
        END
      )
    END AS issue_type
  FROM completed_sessions_tech_issues

)


SELECT
  cp.session_key,
  cp.platform,
  ti.has_tech_issues,
  ti.issue_type
FROM completed_sessions_platform AS cp
INNER JOIN completed_sessions_tech_issue_type AS ti
  ON cp.session_key = ti.session_key

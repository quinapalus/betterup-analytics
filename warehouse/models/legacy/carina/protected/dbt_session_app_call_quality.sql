WITH session AS (

  SELECT * FROM {{ref('dbt_session')}}
  -- select dates after January 2020 product update
  -- to post-session coach and member assessments
  WHERE app_starts_at >= '01/18/2020'

),

completed_sessions_platform AS (
  -- define primary CTE with platform information
  SELECT
    session_key,
    coach_encountered_call_issue,
    coach_feedback_call_quality,
    CASE
      -- if coach said that session was completed on platform, then 'On Platform'
      WHEN coach_reported_completed_on_platform = true THEN 'On Platform'
      ELSE (
        -- Otherwise, if in the next question the coach said they had encountered
        -- a call issue, we say 'Attempted platform'
        CASE
          WHEN coach_encountered_call_issue = true THEN 'Attempted Platform'
          -- If not completed on platform and no issues were encountered
          -- then the call was completed off platform.
          ELSE 'Off Platform' END
      )
      END
    AS platform
  FROM session

),

completed_sessions_tech_issues AS (
  -- define intermediary CTE with tech issue information
  SELECT
    session_key,
    coach_feedback_call_quality,
    platform,
    CASE
      -- complement the logic in the model with inferred call issues:
      -- if the call is off platform do not report on call issues
      WHEN platform = 'Off Platform' THEN NULL
    -- if coach encountered call issues, then we say true,
    -- otherwise false.
      ELSE coach_encountered_call_issue = true
      END
    AS has_tech_issues
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
      -- complement the logic in the model with inferred call issues:
      -- if no issues have been encountered or NULL has been assigned,
      -- do not report on type of issues.
      WHEN has_tech_issues = false OR has_tech_issues IS NULL THEN NULL
      ELSE {{ prioritize_call_issues('coach_feedback_call_quality')}}
      END
    AS issue_type
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

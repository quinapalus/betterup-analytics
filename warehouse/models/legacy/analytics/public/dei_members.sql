{{
  config(
    tags=['eu']
  )
}}

WITH users AS (

  SELECT * FROM {{ref('dbt_users')}}

),

user_info AS (

  SELECT * FROM {{ref('stg_app__users')}}

),

track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

assessments AS (

  SELECT * FROM {{ref('int_app__assessments')}}

),

track_count AS (
  SELECT
    member_id,
    COUNT(DISTINCT track_id) AS track_count
  FROM track_assignments
  GROUP BY member_id
),

member_level AS (

  SELECT
    -- pull most recent self-reported level from onboarding assessment
    user_id AS member_id,
    CASE
      -- responses are either true/false or 1/0
      WHEN responses:is_manager::VARCHAR = 'true'
        OR responses:is_manager::VARCHAR = '1'
            THEN 'manager'
      WHEN responses:is_manager::VARCHAR = 'false'
        OR responses:is_manager::VARCHAR = '0'
            THEN 'individual-contributor'
    END AS level,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY user_id, submitted_at DESC) AS sequence
  FROM assessments
  WHERE is_onboarding
    AND submitted_at IS NOT NULL
    AND responses:is_manager::VARCHAR IS NOT NULL
  QUALIFY sequence = 1

)


SELECT
  m.user_id AS member_id,
  m.created_at,
  m.confirmed_at,
  m.confirmed_at AS activated_at, -- definition of activated since June 2019
  m.confirmed_at IS NOT NULL AS is_activated,
  m.confirmation_sent_at,
  m.completed_member_onboarding_at,
  mi.motivation,
  m.deactivated_at,
  m.next_session_id,
  m.inviter_id,
  m.language,
  m.coaching_language,
  m.organization_id,
  m.manager_id,
  ml.level,
  m.title,
  m.time_zone,
  m.tz_iana,
  m.subregion_m49,
  m.geo,
  tc.track_count
FROM users AS m
INNER JOIN user_info AS mi ON m.user_id = mi.user_id
LEFT OUTER JOIN track_count AS tc ON m.user_id = tc.member_id
LEFT OUTER JOIN member_level AS ml ON m.user_id = ml.member_id
WHERE (array_contains('member'::variant, m.roles) = true OR array_contains('care'::variant, m.roles) = true)
AND m.organization_id NOT IN  (54, 40) -- filter out QA and Coach accounts

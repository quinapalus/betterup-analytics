{{
    config(
        materialized='incremental'
    )
}}

WITH dei_sessions AS (

  SELECT * FROM {{ref('dei_sessions')}}

),

member_dates_on_track AS (

  SELECT * FROM {{ref('dbt_member_dates_on_track')}}

),

member_dates_on_track_sessions AS (

  SELECT * FROM {{ref('dbt_member_dates_on_track_sessions')}}

),

member_dates_with_primary_coach AS (

  SELECT * FROM {{ref('dbt_member_dates_with_primary_coach')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

session_schedule_boundaries AS (

  SELECT
    member_id,
    session_id,
    track_id,
    scheduled_at AS has_upcoming_starts_at,
    -- Whichever is earlier, start or cancellation of session, is considered
    -- the end boundary of when a member had a next session scheduled.
    -- Note: canceled_at is often NULL so sanitize with starts_at
    LEAST(starts_at, COALESCE(canceled_at, starts_at)) AS has_upcoming_ends_at
  FROM dei_sessions

)


SELECT
  md.member_id,
  md.track_id,
  mp.primary_coach_id,
  md.date_day,
  {{bool_or()}}(ss.session_id IS NOT NULL) AS has_upcoming,
  mp.primary_coach_id IS NOT NULL AS is_matched_with_primary_coach,
  ms.completed_session_count_track_to_date,
  t.organization_id,
  t.sfdc_account_id,
  t.is_revenue_generating,
  t.is_external,
  t.contract_id
FROM member_dates_on_track AS md
INNER JOIN tracks AS t
  ON md.track_id = t.track_id
INNER JOIN member_dates_on_track_sessions AS ms
  ON md.member_id = ms.member_id
  AND md.track_id = ms.track_id
  AND md.date_day = ms.date_day
LEFT OUTER JOIN member_dates_with_primary_coach AS mp
  ON md.member_id = mp.member_id
  AND md.date_day = mp.date_day
LEFT OUTER JOIN session_schedule_boundaries AS ss
  ON md.member_id = ss.member_id
  -- One day intervals added as boundary definitions since timestamps
  -- in dei_sessions are truncated to midnight.
 AND ss.has_upcoming_starts_at < dateadd('day', 1, md.date_day)
 AND ss.has_upcoming_ends_at > dateadd('day', 1, md.date_day)
 {% if is_incremental() %}
   WHERE md.date_day > (SELECT MAX(date_day) FROM {{ this }})
 {% endif %}
GROUP BY md.member_id, md.track_id, mp.primary_coach_id, md.date_day, ms.completed_session_count_track_to_date, t.organization_id, t.sfdc_account_id, t.is_revenue_generating, t.is_external, t.contract_id
ORDER BY md.member_id, md.track_id, md.date_day

WITH track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

billable_events AS (

  SELECT * FROM {{ref('app_billable_events')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

valid_external_track_assignments AS (

  SELECT
    *
  FROM track_assignments
  WHERE (ended_at IS NULL OR ended_at > created_at)
    AND track_id IN (SELECT track_id FROM tracks) -- filter for external deployments only

),

overlapping_track_assignments AS (
  -- find all instances of overlapping track assignments per member
  SELECT
    ta1.member_id,
    ta1.track_id AS track_1,
    ta2.track_id AS track_2,
    -- see which track had lesser number of completed sessions over its lifetime and mark as unwanted
    CASE
     WHEN COUNT(DISTINCT be1.billable_event_id) < COUNT(DISTINCT be2.billable_event_id) THEN ta1.track_id
     ELSE ta2.track_id
    END AS unwanted_track_id
  FROM valid_external_track_assignments AS ta1
  INNER JOIN valid_external_track_assignments AS ta2
    ON ta2.member_id = ta1.member_id
      AND ta2.track_id != ta1.track_id
      AND ta2.created_at > (dateadd('second', 1, ta1.created_at))
      AND (ta1.ended_at IS NULL 
          OR ta2.created_at < (dateadd('second', -1, ta1.ended_at))
      )
  LEFT OUTER JOIN billable_events AS be1
    ON ta1.member_id = be1.member_id 
      AND ta1.track_id = be1.track_id
      AND be1.event_type = 'completed_sessions'
      AND be1.event_at > ta1.created_at
      AND (ta1.ended_at IS NULL 
          or be1.event_at < ta1.ended_at)
  LEFT OUTER JOIN billable_events AS be2
    ON ta2.member_id = be2.member_id 
      AND ta2.track_id = be2.track_id
      AND be2.event_type = 'completed_sessions'
      AND be2.event_at > ta2.created_at
      AND (ta2.ended_at IS NULL 
          or be2.event_at < ta2.ended_at)
  GROUP BY ta1.member_id, ta1.track_id, ta2.track_id
  ORDER BY ta1.member_id

)

SELECT
  *
FROM valid_external_track_assignments
WHERE (member_id, track_id) NOT IN (
  -- ignore unwanted member-track pairs in overlapping assignments
  SELECT
    member_id,
    unwanted_track_id
  FROM overlapping_track_assignments

)
